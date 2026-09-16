import concurrent.futures
import logging
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from ceph_utils import COPY_GATE, CopyStalledError, StageImageMissingError
from config import default_vm_pass
from migration_planner import (
    match_flavor,
    order_system_and_data,
    override_key,
    resolve_port_ip,
    vm_channel,
)
#: 单个卷最多重做几次基线全备：暂存卷被反复清掉时不能无限全量拷贝。
MAX_BASELINE_RESEEDS = 2

from state_machine import (
    CLEANUP_CLEANED,
    CLEANUP_PENDING,
    MigrationMode,
    VolumeStatus,
    VolumeTask,
    VmStatus,
    VmTask,
)


class MigrationManager:
    def __init__(
        self,
        source_os,
        target_os,
        ceph_utils,
        can_proceed=None,
        persist=None,
        copy_gate=None,
        cancelled=None,
        cutover_requested=None,
        sync_requested=None,
    ):
        self.source_os = source_os
        self.target_os = target_os
        self.ceph_utils = ceph_utils
        self._can_proceed = can_proceed or (lambda: True)
        self._persist = persist or (lambda: None)
        self._cancelled = cancelled or (lambda: False)
        self._cutover_requested = cutover_requested or (lambda: False)
        self._sync_requested = sync_requested or (lambda: False)
        self.copy_gate = copy_gate or COPY_GATE
        self._sessions: dict[str, Any] = {}
        #: 每个源卷重做基线全备的次数，用于给「暂存卷反复丢失」兜底
        self._baseline_reseeds: dict[str, int] = {}
        self._job_id = ""

    def _check_stop(self) -> None:
        if not self._can_proceed():
            raise RuntimeError("迁移被停机信号中断（当前卷已完成，剩余卷/VM 未处理）")

    @staticmethod
    def _set_phase(vm: VmTask, status: VmStatus, phase: str) -> None:
        vm.status = status
        vm.phase = phase
        logging.info("[MIGRATION] VM %s 阶段 -> %s (%s)", vm.name, phase, status.value)

    def migrate_vm(
        self,
        vm: VmTask,
        options: dict[str, Any],
    ) -> None:
        """Execute the migration state machine for one VM."""
        self._job_id = str(options.get("job_id") or "")
        vm.start()
        try:
            self._migrate_vm_inner(vm, options)
        except Exception as exc:  # noqa: BLE001 - keep VM-level failures isolated
            logging.exception("[MIGRATION] VM %s 迁移失败", vm.name)
            self._cleanup_failed_vm(vm)
            if self._cancelled():
                # 用户主动取消时按取消归类，避免一堆红色失败误导判断。
                vm.mark_cancelled("任务被用户取消")
            else:
                vm.mark_failed(str(exc))
        else:
            vm.mark_success()
            logging.info(
                "[MIGRATION] VM %s 迁移成功：源 server=%s 目标 server=%s "
                "卷数=%s 端口=%s 耗时=%ss",
                vm.name,
                vm.source_server_id,
                vm.target_server_id,
                len(vm.volumes),
                len(vm.target_network_ports),
                vm.duration_seconds,
            )

    def _migrate_vm_inner(
        self,
        vm: VmTask,
        options: dict[str, Any],
    ) -> None:
        # 网络覆盖配置必须在分流之前装载：中转机通道也要按源 IP 匹配目标网络/子网，
        # 否则页面上的映射会被当成"未配置"。
        self._vm_network_overrides = (
            options.get("vm_network_overrides") or {}
        )
        if vm_channel(options, vm.name, vm.source_server_id).strip().lower() == "relay":
            return self._migrate_vm_via_relay(vm, options)
        self._check_stop()
        # 1. collect source info
        self._set_phase(vm, VmStatus.CREATING_TARGET_BFV, "preparing_source")
        source_server = self._resolve_source_server(vm)
        logging.info(
            "[MIGRATION] VM %s 源端信息: server=%s status=%s",
            vm.name,
            source_server.id,
            getattr(source_server, "status", "?"),
        )

        source_flavor = self.source_os.get_server_flavor_spec(source_server)
        target_flavor_id = self._resolve_flavor_id(vm, source_flavor)
        logging.info(
            "[MIGRATION] VM %s 源 flavor=%s (cpu=%s mem=%sMB disk=%sGB) -> 目标 flavor=%s",
            vm.name,
            source_flavor.get("name"),
            source_flavor.get("vcpus"),
            source_flavor.get("ram"),
            source_flavor.get("disk"),
            target_flavor_id,
        )

        # 2. create target ports that keep source fixed IPs
        port_ids = self._create_target_ports(vm)
        if not port_ids:
            raise RuntimeError("目标 VM 没有可用端口，源 VM 必须至少有一个固定 IP")

        # 3. create blank target data volumes and gather source layout
        logging.info("[MIGRATION] VM %s 采集源卷清单...", vm.name)
        source_entries = self.source_os.get_server_volumes_with_device(
            source_server.id
        )
        ordered_source = order_system_and_data(source_entries)
        source_boot_entries = [
            entry for entry in ordered_source if entry["is_bootable"]
        ]
        source_data_entries = [
            entry for entry in ordered_source if not entry["is_bootable"]
        ]
        if len(source_boot_entries) != 1:
            raise RuntimeError(
                f"源 VM {vm.name} 必须有且只有一个系统盘，当前 {len(source_boot_entries)} 个"
            )
        source_boot = source_boot_entries[0]
        logging.info(
            "[MIGRATION] VM %s 源卷布局: 系统盘=%s(%sGB) 数据盘=%s",
            vm.name,
            source_boot["volume_id"],
            source_boot["size"],
            [
                (entry["volume_id"], entry["size"], entry["device"])
                for entry in source_data_entries
            ],
        )

        target_data_volume_ids = []
        for source_data in source_data_entries:
            self._check_stop()
            logging.info(
                "[MIGRATION] VM %s 创建目标空白数据卷 (对应源 %s, %sGB)...",
                vm.name,
                source_data["volume_id"],
                source_data["size"],
            )
            target_volume = self.target_os.create_blank_volume(
                name=f"{vm.name}-{source_data.get('name') or source_data['volume_id']}-mig",
                size=source_data["size"],
                volume_type=options.get("target_volume_type"),
            )
            self.target_os.wait_volume_status(target_volume.id)
            target_data_volume_ids.append(target_volume.id)
            vm.created_resources.append(f"volume:{target_volume.id}")
            logging.info(
                "[MIGRATION] VM %s 数据卷创建完成: %s (源 %s)",
                vm.name,
                target_volume.id,
                source_data["volume_id"],
            )

        # 4. create target BFV VM
        logging.info(
            "[MIGRATION] VM %s 创建 BFV VM: 镜像=%s flavor=%s 端口=%s "
            "系统盘=%sGB 数据卷=%s AZ=%s",
            vm.name,
            vm.target_image,
            target_flavor_id,
            port_ids,
            source_boot["size"],
            target_data_volume_ids,
            vm.target_az,
        )
        target_image = self.target_os.find_image(vm.target_image)
        if not target_image:
            raise RuntimeError(f"目标镜像 {vm.target_image} 不存在")
        server = self.target_os.create_bfv_server(
            name=vm.name,
            image_id=target_image.id,
            flavor_id=target_flavor_id,
            port_ids=port_ids,
            boot_volume_size=source_boot["size"],
            data_volume_ids=target_data_volume_ids,
            availability_zone=vm.target_az,
            admin_password=(options.get("admin_password") or "").strip()
            or default_vm_pass,
            volume_type=options.get("target_volume_type"),
        )
        vm.target_server_id = server.id
        vm.created_resources.append(f"server:{server.id}")
        logging.info(
            "[MIGRATION] VM %s BFV 创建成功 server=%s，等待 ACTIVE...",
            vm.name,
            server.id,
        )
        self.target_os.wait_server_status(
            server.id,
            target="ACTIVE",
            fail_states={"ERROR", "SUSPENDED", "PAUSED"},
        )
        logging.info("[MIGRATION] VM %s 目标 server=%s 已 ACTIVE，开始关机", vm.name, server.id)
        self._set_phase(vm, VmStatus.STOPPING_TARGET, "stopping_target")
        self._stop_and_wait(vm.target_server_id, self.target_os)

        # 迁移功能项只在这一处选择数据搬运器；其余生命周期步骤全部共用。
        mover = self._mover_for(vm)

        # 5. map source and target volumes（只读，提前到停源之前）
        self._set_phase(vm, VmStatus.COLLECTING_VOLUMES, "collecting_volumes")
        logging.info("[MIGRATION] VM %s 采集目标卷信息并建立映射...", vm.name)
        target_entries = self.target_os.get_server_volumes_with_device(
            vm.target_server_id
        )
        self._build_volume_tasks(
            vm,
            source_boot_entries=source_boot_entries,
            source_data_entries=source_data_entries,
            target_entries=target_entries,
            target_data_volume_ids=target_data_volume_ids,
            source_pool=str(options.get("source_ceph_pool") or ""),
            target_pool=str(options.get("target_ceph_pool") or ""),
        )
        logging.info(
            "[MIGRATION] VM %s 卷映射: %s",
            vm.name,
            [
                (task.source_rbd_name, task.target_rbd_name, task.size)
                for task in vm.volumes
            ],
        )

        # 6. 预拷贝：全量为空操作；增量为基像 + 多轮增量（源 VM 仍运行）
        mover.prepare(vm, options)

        # 7. stop source for data consistency
        self._set_phase(vm, VmStatus.STOPPING_SOURCE, "stopping_source")
        self._stop_and_wait(vm.source_server_id, self.source_os)

        # 8. copy/replace every volume
        self._set_phase(vm, VmStatus.COPYING_VOLUMES, "copying_volumes")
        self._check_stop()
        self._copy_volumes(
            vm,
            max_workers=int(options.get("volume_concurrency") or 1),
            rate_limit_mb_s=float(options.get("rate_limit_mb_s") or 0),
            mover=mover,
        )
        self._check_stop()
        if not vm.can_start_target():
            raise RuntimeError("存在 RBD 替换失败的卷，已跳过目标 VM 开机")
        logging.info(
            "[MIGRATION] VM %s 全部 %s 个卷替换成功，启动目标 VM...",
            vm.name,
            len(vm.volumes),
        )

        # 8. start target only after all volumes are replaced
        self._check_stop()
        self._set_phase(vm, VmStatus.STARTING_TARGET, "starting_target")
        logging.info("[MIGRATION] VM %s 启动目标 server=%s", vm.name, vm.target_server_id)
        self.target_os.start_server(vm.target_server_id)
        self._set_phase(vm, VmStatus.VERIFYING, "verifying")
        self.target_os.wait_server_status(
            vm.target_server_id,
            target="ACTIVE",
            fail_states={"ERROR", "SUSPENDED", "PAUSED"},
        )
        self._set_phase(vm, VmStatus.SUCCESS, "success")

    # ---------- helpers ----------

    def _migrate_vm_via_relay(self, vm: VmTask, options: dict[str, Any]) -> None:
        """中转机通道：停源 → 逐卷快照派生并拷贝 → 用目标卷建 VM。"""
        mover_factory = options.get("relay_mover_factory")
        if mover_factory is None:
            raise RuntimeError("未配置中转机搬运器，无法走中转机通道")

        self._check_stop()
        source_server = self._resolve_source_server(vm)
        source_flavor = self.source_os.get_server_flavor_spec(source_server)
        target_flavor_id = self._resolve_flavor_id(vm, source_flavor)

        port_ids = self._create_target_ports(vm)
        if not port_ids:
            raise RuntimeError("目标 VM 没有可用端口，源 VM 必须至少有一个固定 IP")

        entries = order_system_and_data(
            self.source_os.get_server_volumes_with_device(source_server.id)
        )
        boot_entries = [entry for entry in entries if entry["is_bootable"]]
        data_entries = [entry for entry in entries if not entry["is_bootable"]]
        if len(boot_entries) != 1:
            raise RuntimeError(
                f"源 VM {vm.name} 必须有且只有一个系统盘，当前 {len(boot_entries)} 个"
            )

        # 中转机通道要求源数据在拷贝期间不变，因此先停源 VM 再打快照。
        self._set_phase(vm, VmStatus.STOPPING_SOURCE, "relay_stopping_source")
        self._stop_and_wait(source_server.id, self.source_os)

        mover = mover_factory(vm, options)
        volume_types = (options.get("volume_overrides") or {}).get(
            override_key(vm.source_server_id, vm.name)
        ) or {}
        self._set_phase(vm, VmStatus.COPYING_VOLUMES, "relay_copying")
        target_volume_ids: list[str] = []
        for index, entry in enumerate([boot_entries[0]] + data_entries):
            override = volume_types.get(entry["volume_id"]) or {}
            if isinstance(override, str):  # 兼容只写了目标类型的旧格式
                override = {"target": override}
            result = mover.move(
                volume=SimpleNamespace(
                    source_volume_id=entry["volume_id"],
                    size=int(entry.get("size") or 0),
                ),
                vm_name=vm.name,
                index=index,
                source_volume_type=str(override.get("source") or "").strip() or None,
                target_volume_type=str(override.get("target") or "").strip() or None,
            )
            target_volume_id = str(result["target_volume_id"])
            if index == 0:
                # 空白启动盘默认不可启动，先置位再建机，否则 Nova 返回
                # "Block Device ... is not bootable"。
                mover.lifecycle.mark_bootable(volume_id=target_volume_id)
            target_volume_ids.append(target_volume_id)

        self._set_phase(vm, VmStatus.CREATING_TARGET_BFV, "relay_creating_target_vm")
        server = self.target_os.create_server_from_volumes(
            name=f"{vm.name}-relay",
            flavor_id=target_flavor_id,
            port_ids=port_ids,
            boot_volume_id=target_volume_ids[0],
            data_volume_ids=target_volume_ids[1:],
            availability_zone=vm.target_az,
            admin_password=str(options.get("admin_password") or default_vm_pass),
        )
        vm.target_server_id = server.id
        vm.created_resources.append({"type": "server", "id": server.id})
        vm.target_network_ports = list(vm.target_network_ports or port_ids)

        self._set_phase(vm, VmStatus.STARTING_TARGET, "starting_target")
        # create 返回后实例可能还在 BUILD（甚至短暂 404），由云侧自动开机；
        # 立刻调 os-start 只会撞 404/409，把一次成功的建机判成失败。
        self.target_os.wait_server_booted(server.id)
        self._set_phase(vm, VmStatus.VERIFYING, "verifying")

    def _resolve_source_server(self, vm: VmTask):
        if vm.source_server_id:
            server = self.source_os.get_server_detail(vm.source_server_id)
            if not server:
                raise RuntimeError(f"源 VM {vm.source_server_id} 不存在")
            return server
        server = self.source_os.get_server_by_name(vm.name)
        if not server:
            raise RuntimeError(f"源 VM {vm.name} 不存在")
        vm.source_server_id = server.id
        return server

    def _resolve_flavor_id(self, vm: VmTask, source_flavor: dict[str, Any]) -> str:
        if vm.target_flavor:
            flavor = self.target_os.find_flavor(vm.target_flavor)
            if not flavor:
                raise RuntimeError(f"目标 flavor {vm.target_flavor} 不存在")
            return flavor.id
        target_flavors = [
            flavor.to_dict()
            for flavor in self.target_os.list_flavors()
        ]
        matched = match_flavor(source_flavor, target_flavors)
        if not matched:
            raise RuntimeError(
                "目标端未找到与源 flavor 匹配的规格，请在页面指定目标 flavor"
            )
        return matched["id"]

    def _create_target_ports(
        self,
        vm: VmTask,
    ) -> list[str]:
        vm_overrides = getattr(self, "_vm_network_overrides", {}) or {}
        source_addresses = self.source_os.get_server_addresses(vm.source_server_id)
        vm_entries = (
            vm_overrides.get(override_key(vm.source_server_id, vm.name)) or []
        ) or []
        port_ids = []
        for network_name, entries in source_addresses.items():
            fixed_ips = [
                entry
                for entry in entries
                if entry.get("version") == 4 and entry.get("type") != "floating"
            ]
            for fixed_ip_entry in fixed_ips:
                fixed_ip = fixed_ip_entry["addr"]
                vm_entry = next(
                    (
                        entry
                        for entry in vm_entries
                        if entry.get("source_ip") == fixed_ip
                    ),
                    None,
                )
                if not vm_entry or not vm_entry.get("target_network"):
                    vm_entry = next(
                        (
                            entry
                            for entry in vm_entries
                            if entry.get("source_network") == network_name
                            and entry.get("target_network")
                        ),
                        None,
                    )
                if (
                    not vm_entry
                    or not vm_entry.get("target_network")
                    or not vm_entry.get("target_subnet")
                ):
                    raise RuntimeError(
                        f"VM {vm.name} 源网络 {network_name} 源 IP {fixed_ip} "
                        "未配置目标网络/子网，请先在迁移清单中为该 VM 选择目标网络/子网"
                    )
                target_network = self.target_os.find_network(
                    vm_entry["target_network"]
                )
                target_subnet = self.target_os.find_subnet(
                    vm_entry["target_subnet"]
                )
                if not target_network or not target_subnet:
                    raise RuntimeError(
                        f"目标网络/子网 {vm_entry.get('target_network')}/"
                        f"{vm_entry.get('target_subnet')} 不存在"
                    )
                vm.source_ips.append(fixed_ip)
                resolved_ip = resolve_port_ip(
                    source_ip=fixed_ip,
                    target_cidr=target_subnet.cidr,
                    target_ip=vm_entry.get("target_ip") or "",
                )
                port = self.target_os.create_port_with_fixed_ip(
                    network_id=target_network.id,
                    subnet_id=target_subnet.id,
                    fixed_ip=resolved_ip,
                )
                port_ids.append(port.id)
                vm.target_network_ports.append(port.id)
                if resolved_ip:
                    vm.target_ips.append(resolved_ip)
                vm.created_resources.append(f"port:{port.id}")
                logging.info(
                    "[MIGRATION] VM %s 端口创建: 源网络=%s 源IP=%s -> "
                    "目标网络=%s 子网=%s 目标IP=%s 端口=%s",
                    vm.name,
                    network_name,
                    fixed_ip,
                    target_network.id,
                    target_subnet.id,
                    resolved_ip or "(自动分配)",
                    port.id,
                )
        return port_ids

    @staticmethod
    def _stop_and_wait(server_id: str, os_utils) -> None:
        server = os_utils.get_server_detail(server_id)
        if server.status != "SHUTOFF":
            logging.info("[MIGRATION] 停止 VM/实例 %s (当前状态 %s)", server_id, server.status)
            os_utils.stop_server(server_id)
        os_utils.wait_server_status(
            server_id,
            target="SHUTOFF",
            fail_states={"ERROR", "SUSPENDED", "PAUSED"},
        )
        logging.info("[MIGRATION] 实例 %s 已关机", server_id)

    def _build_volume_tasks(
        self,
        vm: VmTask,
        source_boot_entries: list[dict[str, Any]],
        source_data_entries: list[dict[str, Any]],
        target_entries: list[dict[str, Any]],
        target_data_volume_ids: list[str],
        source_pool: str = "",
        target_pool: str = "",
    ) -> None:
        target_boot_entries = [
            entry
            for entry in target_entries
            if entry["volume_id"] not in set(target_data_volume_ids)
        ]
        if len(source_boot_entries) != 1 or len(target_boot_entries) != 1:
            raise RuntimeError("源/目标系统盘数量不一致")

        target_data_by_id = {
            entry["volume_id"]: entry
            for entry in target_entries
            if entry["volume_id"] in set(target_data_volume_ids)
        }
        tasks = []
        pairs = [("boot", source_boot_entries[0], target_boot_entries[0])]
        pairs.extend(
            ("data", source_data_entries[index], target_data_by_id[volume_id])
            for index, volume_id in enumerate(target_data_volume_ids)
        )
        for role, source_entry, target_entry in pairs:
            source_id = source_entry["volume_id"]
            target_id = target_entry["volume_id"]
            tasks.append(
                VolumeTask(
                    source_volume_id=source_id,
                    source_rbd_name=f"volume-{source_id}",
                    target_volume_id=target_id,
                    target_rbd_name=f"volume-{target_id}",
                    size=source_entry.get("size") or target_entry.get("size") or 0,
                    source_pool=source_pool,
                    target_pool=target_pool,
                    mode=vm.mode,
                )
            )
        vm.volumes = tasks

    def _volume_progress_cb(self, vm: VmTask, volume: VolumeTask):
        """统一的卷拷贝进度回调：全量与增量（基像/增量轮次）共用同一套语义。

        total 为空时按卷容量估算，保证任何路径都能给出稳定的百分比与速率。
        """
        last_log = [time.time()]

        def on_progress(
            copied: int,
            total: int | None,
            bytes_per_sec: float,
        ) -> None:
            total_bytes = total if total else (
                volume.size * 1024 * 1024 * 1024 if volume.size else 0
            )
            percent = (
                min(100.0, copied / total_bytes * 100.0) if total_bytes else 0.0
            )
            volume.progress_percent = round(max(0.0, percent), 1)
            if bytes_per_sec > 0:
                volume.throughput_mb_s = round(bytes_per_sec / (1024 * 1024), 1)
            now = time.time()
            if now - last_log[0] < 5:
                return
            last_log[0] = now
            label = f"{volume.progress_label} " if volume.progress_label else ""
            logging.info(
                "[MIGRATION] VM %s 卷 %s -> %s %s拷贝中 %.1f%%（%.2f/%.2f GiB）"
                " %.1f MiB/s",
                vm.name,
                volume.source_rbd_name,
                volume.target_rbd_name,
                label,
                volume.progress_percent,
                copied / (1024 * 1024 * 1024),
                total_bytes / (1024 * 1024 * 1024) if total_bytes else 0,
                volume.throughput_mb_s or 0,
            )

        return on_progress

    def _copy_volumes(
        self,
        vm: VmTask,
        max_workers: int,
        rate_limit_mb_s: float = 0.0,
        mover=None,
    ) -> None:
        mover = mover or self._mover_for(vm)
        rate_limit_bytes_per_sec = (
            max(0.0, float(rate_limit_mb_s or 0.0)) * 1024 * 1024
        )

        def copy_one(volume: VolumeTask) -> None:
            if not self._can_proceed():
                volume.status = VolumeStatus.FAILED
                volume.error = "迁移被停机信号中断"
                self._persist()
                return
            volume.status = VolumeStatus.COPYING
            # 换入前的等待可能卡在门控上，标签要如实反映这一步在干什么：
            # 全量是"全量拷贝"，增量是停机后的"末轮增量"。
            self._set_volume_progress_label(
                volume,
                "全量拷贝" if volume.mode == MigrationMode.FULL else "末轮增量",
            )  # 内部会 persist
            started = time.time()
            logging.info(
                "[MIGRATION] VM %s 开始替换卷 %s(%sGB) -> %s 限速=%sMiB/s",
                vm.name,
                volume.source_rbd_name,
                volume.size,
                volume.target_rbd_name,
                rate_limit_mb_s or "不限",
            )

            on_progress = self._volume_progress_cb(vm, volume)

            ok = False
            attempts = 0
            while attempts < 2:
                attempts += 1
                if attempts > 1:
                    if not self._can_proceed():
                        volume.status = VolumeStatus.FAILED
                        volume.error = "迁移被停机信号中断，未执行自动重试"
                        self._persist()
                        return
                    logging.warning(
                        "[MIGRATION] VM %s 卷 %s -> %s 第 1 次替换失败，"
                        "自动重试（重跑会重新校验源/目标 RBD 大小、"
                        "watcher、快照，并自动清理残留 %s-mig-stage）",
                        vm.name,
                        volume.source_rbd_name,
                        volume.target_rbd_name,
                        volume.target_rbd_name,
                    )
                    volume.error = (
                        "RBD 替换失败(第1次)，准备自动重试: "
                        f"{volume.source_rbd_name} -> {volume.target_rbd_name}"
                    )
                    self._persist()

                self.copy_gate.acquire(
                    can_proceed=self._can_proceed,
                    owner=f"{vm.name}/{volume.source_rbd_name}",
                )
                try:
                    ok = mover.finish(
                        volume, on_progress, rate_limit_bytes_per_sec
                    )
                except CopyStalledError as exc:
                    # 停滞是集群侧问题，重试同一个卷没有意义：先把卷标失败，再把
                    # 原因抛给 migrate_vm，让 VM 错误文案带上真实根因（而不是只
                    # 留一个"RBD 替换失败"），名额仍由下面的 finally 释放。
                    volume.status = VolumeStatus.FAILED
                    volume.error = str(exc)
                    self._persist()
                    raise
                finally:
                    self.copy_gate.release()
                if ok:
                    break
            if not ok:
                mover.on_failure(volume)
            if ok:
                volume.status = VolumeStatus.SUCCESS
                volume.progress_percent = 100.0
                volume.throughput_mb_s = None
                # 第一次失败留下的"准备自动重试"文案必须清掉，否则卷已成功，
                # 前端仍显示红色错误，误判为失败。
                volume.error = None
            else:
                volume.status = VolumeStatus.FAILED
                volume.error = f"RBD 替换失败: {volume.source_rbd_name} -> {volume.target_rbd_name}"
            logging.info(
                "[MIGRATION] VM %s 卷 %s -> %s %s，耗时 %.1fs",
                vm.name,
                volume.source_rbd_name,
                volume.target_rbd_name,
                "成功" if ok else "失败",
                time.time() - started,
            )
            self._persist()

        if max_workers <= 1 or len(vm.volumes) <= 1:
            for volume in vm.volumes:
                copy_one(volume)
                self._check_stop()
            return
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:
            futures = []
            for volume in vm.volumes:
                if not self._can_proceed():
                    volume.status = VolumeStatus.FAILED
                    volume.error = "迁移被停机信号中断"
                    continue
                futures.append(executor.submit(copy_one, volume))
            for future in concurrent.futures.as_completed(futures):
                future.result()
            self._check_stop()

    # ---------- 迁移功能项：数据搬运器 ----------

    def _mover_for_mode(self, mode: MigrationMode):
        """整个流水线唯一一处 mode 判断。"""
        if mode == MigrationMode.INCREMENTAL:
            return IncrementalVolumeMover(self)
        return FullVolumeMover(self)

    def _mover_for(self, vm: VmTask):
        return self._mover_for_mode(vm.mode)

    def _record_snapshot(self, volume: VolumeTask, snap_name: str) -> None:
        volume.snapshots.append(
            {
                "name": snap_name,
                "state": "created",
                "at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._persist()

    def _mark_snapshot_removed(self, volume: VolumeTask, snap_name: str) -> None:
        for record in volume.snapshots:
            if record.get("name") == snap_name:
                record["state"] = "removed"
        self._persist()

    def _precopy_volumes(self, vm: VmTask, options: dict[str, Any]) -> None:
        """源 VM 仍在运行时完成基像与增量轮次（仅增量模式调用）。"""
        self._set_phase(vm, VmStatus.PRECOPYING, "precopying")
        rate_limit_bytes_per_sec = (
            max(0.0, float(options.get("rate_limit_mb_s") or 0.0)) * 1024 * 1024
        )
        vm.delta_rounds_max = int(options.get("delta_rounds") or 0)
        vm.delta_threshold_bytes = int(
            max(0.0, float(options.get("delta_threshold_mb") or 0.0)) * 1024 * 1024
        )
        vm.delta_cutover_mode = str(options.get("cutover_mode") or "auto")
        vm.delta_interval_seconds = max(0.0, float(options.get("delta_interval_seconds") or 0.0))
        # 阶段一：先把"所有盘"各做一次基像全备。原来的实现是"卷 1 全备 + N 轮
        # 增量 → 卷 2 全备 + N 轮增量"，后面的盘要等前面的盘把所有轮次跑完才
        # 开始，既拖长整体窗口，又让后开始的盘在切换时背着巨大的增量。
        self._seed_all_volumes(
            vm,
            max_workers=int(options.get("volume_concurrency") or 1),
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )
        # 阶段二：所有盘按轮次对齐做增量（每轮每盘各一次），而不是单卷榨干。
        self._sync_volumes_rounds(
            vm,
            rounds=int(options.get("delta_rounds") or 0),
            threshold_mb=float(options.get("delta_threshold_mb") or 0),
            interval_seconds=float(options.get("delta_interval_seconds") or 0),
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            cutover_mode=str(options.get("cutover_mode") or "auto"),
            max_workers=int(options.get("volume_concurrency") or 1),
        )

    def _seed_all_volumes(
        self,
        vm: VmTask,
        *,
        max_workers: int,
        rate_limit_bytes_per_sec: float,
    ) -> None:
        """阶段一：所有卷的基像全备（受 volume_concurrency 与拷贝门控约束）。"""

        def seed(volume: VolumeTask) -> None:
            self.copy_gate.acquire(
                can_proceed=self._can_proceed,
                owner=f"{vm.name}/{volume.source_rbd_name}",
            )
            try:
                self._seed_volume(
                    vm, volume, rate_limit_bytes_per_sec=rate_limit_bytes_per_sec
                )
            finally:
                self.copy_gate.release()

        self._run_volume_workers(seed, list(vm.volumes), max_workers)

    def _seed_volume(
        self,
        vm: VmTask,
        volume: VolumeTask,
        *,
        rate_limit_bytes_per_sec: float,
    ) -> None:
        """单卷基像全备；调用方负责持有拷贝门控。"""
        self._check_stop()
        self._set_volume_progress_label(volume, "基线全量")
        # 必须在这里就置成 copying：卷状态要等 `seed_volume` 返回才更新的话，
        # 整个基线全备（大卷几十分钟）期间卷都是 pending，页面按状态判空
        # 就只在拷贝结束的瞬间才画出进度条，看起来像"到 100% 才有进度"。
        volume.status = VolumeStatus.COPYING
        self._persist()
        session = self.ceph_utils.seed_volume(
            volume.source_rbd_name,
            volume.target_rbd_name,
            self._job_id,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            progress_cb=self._volume_progress_cb(vm, volume),
            on_phase=lambda label, keep=False, vol=volume: (
                self._set_volume_progress_label(vol, label, keep)
            ),
            on_snapshot=lambda name, vol=volume: self._record_snapshot(vol, name),
        )
        self._sessions[volume.source_volume_id] = session
        volume.layout = session.layout
        self._set_volume_progress_label(volume, "基线完成", keep_percent=True)
        self._mark_volume_ready(volume)

    def _mark_volume_ready(self, volume: VolumeTask) -> None:
        """一卷的本次搬运已经写完：进度收敛到 100%、清掉速率，卷转"已就绪"。

        增量轮次的进度总量是 `rbd diff` 的预估值，比 `rbd export-diff` 的实际
        载荷略大（线上见过预估 5.75 GiB / 实传 5.71 GiB），回调因此永远到不了
        100%。但"这一轮已经搬完"是确定的事实，不收尾就会出现 VM 已进入
        `awaiting_cutover`、卷行却停在 99.2% + 旧速率 + `copying` 的自相矛盾画面。
        """
        volume.status = VolumeStatus.READY
        volume.progress_percent = 100.0
        volume.throughput_mb_s = None
        self._persist()

    def _sync_volumes_rounds(
        self,
        vm: VmTask,
        *,
        rounds: int,
        threshold_mb: float,
        interval_seconds: float,
        rate_limit_bytes_per_sec: float,
        cutover_mode: str,
        max_workers: int,
    ) -> None:
        """阶段二：增量同步，每轮让所有卷各做一次增量。

        `auto`：按 `delta_rounds`、阈值与间隔自动跑，收敛后返回，调用方随即
        停源做末轮切换。
        `manual`：基线全备完成后**不自动跑任何轮次**，直接进入"待切换"待命；
        用户点「同步一次」才做一轮，点「开始切换」才返回并进入停机切换。
        每点一次才产生一轮快照，不会在后台按间隔无脑堆快照。
        """
        threshold_bytes = int(threshold_mb * 1024 * 1024)
        completed = 0

        def run_round(label: str) -> int:
            """一轮同步；返回本轮所有卷里最大的增量字节数（用于收敛判断）。"""
            copied_by_volume: dict[str, int] = {}

            def sync(volume: VolumeTask) -> None:
                if self._sessions.get(volume.source_volume_id) is None:
                    return
                self._check_stop()
                self._set_volume_progress_label(volume, label)  # 内部会 persist
                # 上一轮结束时卷是 READY，本轮重新开跑要转回 copying，
                # 否则进度条会一边走一边挂着"已就绪"的徽标。
                volume.status = VolumeStatus.COPYING
                self._persist()
                self.copy_gate.acquire(
                    can_proceed=self._can_proceed,
                    owner=f"{vm.name}/{volume.source_rbd_name}",
                )
                try:
                    copied = self._sync_round_with_reseed(
                        vm,
                        volume,
                        rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
                    )
                finally:
                    self.copy_gate.release()
                copied_by_volume[volume.source_volume_id] = int(copied or 0)
                # 本轮字节数已经全部落盘：进度收敛到 100%，回"已就绪"等下一步。
                self._mark_volume_ready(volume)

            self._run_volume_workers(sync, list(vm.volumes), max_workers)
            largest = max(copied_by_volume.values()) if copied_by_volume else 0
            # 供作业详情展示「已同步 N 轮 / 最近一轮增量」；手动同步同样计数。
            vm.delta_rounds_done += 1
            vm.delta_last_bytes = largest
            return largest

        if cutover_mode == "manual":
            self._await_manual_actions(vm, run_round)
            return

        while True:
            if interval_seconds > 0 and completed:
                # 轮次之间在等间隔，进度条会一直停在上一轮的 100%：
                # 必须把"在等什么"说出来，否则看起来像卡死。
                self._label_volumes(
                    vm,
                    f"等待下一轮（{int(interval_seconds)}s）",
                    keep_percent=True,
                )
                self._sleep_with_stop(interval_seconds)
            self._check_stop()
            label = f"增量 {completed + 1}/{rounds}" if rounds else "增量同步"
            largest = run_round(label)
            completed += 1
            self._persist()
            if completed >= max(rounds, 1):
                return
            if threshold_bytes and largest < threshold_bytes:
                logging.info(
                    "[MIGRATION] VM %s 增量已收敛（本轮最大增量 %s 字节 < 阈值 %s），提前结束预拷贝",
                    vm.name,
                    largest,
                    threshold_bytes,
                )
                return

    def _sync_round_with_reseed(
        self,
        vm: VmTask,
        volume: VolumeTask,
        *,
        rate_limit_bytes_per_sec: float,
    ) -> int:
        """跑一轮增量；暂存卷已丢失时就地重做基线全备再重跑本轮。

        暂存卷只在基线全备时创建，中途消失（后台 GC 误回收、人工 `rbd rm`）在
        旧实现里直接判定 VM 失败，用户只能整台从头再来。这里补做一次基线全备，
        把本轮增量补齐后继续等人工切换；重做次数有上限，避免"刚建好又被清掉"
        时反复全量拷贝。调用方负责持有拷贝门控。
        """
        while True:
            try:
                return self._run_volume_round(vm, volume, rate_limit_bytes_per_sec)
            except StageImageMissingError:
                reseeded = self._baseline_reseeds.get(volume.source_volume_id, 0)
                if reseeded >= MAX_BASELINE_RESEEDS:
                    # 刚重做完又没了，说明有东西在持续清这个卷：再全量拷贝只是
                    # 白烧几小时带宽，直接把原错误抛给上层，让任务显式失败。
                    logging.error(
                        "[MIGRATION] VM %s 卷 %s 的暂存卷反复丢失（已重做 %d 次基线"
                        "全备），不再重试",
                        vm.name,
                        volume.source_rbd_name,
                        reseeded,
                    )
                    raise
                self._baseline_reseeds[volume.source_volume_id] = reseeded + 1
                logging.warning(
                    "[MIGRATION] VM %s 卷 %s 的暂存卷已丢失，重做基线全备后重跑本轮"
                    "增量（第 %d/%d 次）",
                    vm.name,
                    volume.source_rbd_name,
                    reseeded + 1,
                    MAX_BASELINE_RESEEDS,
                )
                self._set_volume_progress_label(volume, "暂存卷丢失，重做基线全量")
                self._drop_stale_migration_snapshots(volume)
                self._seed_volume(
                    vm, volume, rate_limit_bytes_per_sec=rate_limit_bytes_per_sec
                )

    def _run_volume_round(
        self,
        vm: VmTask,
        volume: VolumeTask,
        rate_limit_bytes_per_sec: float,
    ) -> int:
        """单卷一轮增量；会话在每次调用时重新取，重做基线后会换成新会话。"""
        session = self._sessions.get(volume.source_volume_id)
        if session is None:
            return 0
        copied = self.ceph_utils.sync_volume_round(
            session,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            progress_cb=self._volume_progress_cb(vm, volume),
            on_snapshot=lambda name, vol=volume: self._record_snapshot(vol, name),
        )
        return int(copied or 0)

    def _drop_stale_migration_snapshots(self, volume: VolumeTask) -> None:
        """重做基线前清掉本 job 在源卷上的旧快照，否则快照名会与新建的撞车。"""
        if not self._job_id:
            return
        try:
            removed = self.ceph_utils.prune_source_snapshots(
                volume.source_rbd_name, self._job_id, keep=0
            )
        except Exception:  # noqa: BLE001 - 清不掉时交给下面建快照自己报错
            logging.exception(
                "[MIGRATION] 重做基线前清理旧迁移快照失败 %s",
                volume.source_rbd_name,
            )
            return
        for snap_name in removed:
            self._mark_snapshot_removed(volume, snap_name)

    def _await_manual_actions(self, vm: VmTask, run_round) -> None:
        """手动模式待命：只响应「同步一次」与「开始切换」，不自动跑轮次。"""
        self._mark_awaiting_cutover(vm)
        self._label_volumes(vm, "已就绪，等待同步", keep_percent=True)
        while True:
            self._check_stop()
            if self._cutover_requested():
                logging.info(
                    "[MIGRATION] VM %s 收到切换指令，停止增量同步并进入停机切换",
                    vm.name,
                )
                return
            if self._sync_requested():
                vm.sync_requested = False
                self._set_phase(vm, VmStatus.PRECOPYING, "syncing")
                logging.info("[MIGRATION] VM %s 收到同步指令，开始一轮增量", vm.name)
                run_round("增量同步")
                self._persist()
                self._mark_awaiting_cutover(vm)
                self._label_volumes(vm, "已就绪，等待同步", keep_percent=True)
                continue
            self._sleep_with_stop(2.0)

    def _label_volumes(
        self, vm: VmTask, label: str, *, keep_percent: bool = False
    ) -> None:
        """给该 VM 所有卷统一换阶段标签（用于"在等什么"这类非拷贝阶段）。"""
        for volume in vm.volumes:
            self._set_volume_progress_label(volume, label, keep_percent)

    def _mark_awaiting_cutover(self, vm: VmTask) -> None:
        """数据就绪后停在等待切换：源机保持运行，增量只在用户点击时进行。"""
        if vm.status == VmStatus.AWAITING_CUTOVER:
            return
        self._set_phase(vm, VmStatus.AWAITING_CUTOVER, "awaiting_cutover")
        logging.info(
            "[MIGRATION] VM %s 增量已就绪，等待人工确认切换（源机仍在运行）",
            vm.name,
        )

    def _sleep_with_stop(self, seconds: float) -> None:
        """可被取消/停机打断的等待，避免长 sleep 让取消操作迟迟不生效。"""
        deadline = time.time() + max(0.0, seconds)
        while True:
            self._check_stop()
            remaining = deadline - time.time()
            if remaining <= 0:
                return
            time.sleep(min(2.0, remaining))

    def _run_volume_workers(
        self,
        action,
        volumes: list[VolumeTask],
        max_workers: int,
    ) -> None:
        """按 volume_concurrency 并发跑卷级动作；并发度 1 时保持串行语义。"""
        if max_workers <= 1 or len(volumes) <= 1:
            for volume in volumes:
                action(volume)
            return
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:
            futures = [executor.submit(action, volume) for volume in volumes]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def _finalize_incremental_volume(
        self,
        volume: VolumeTask,
        on_progress,
        rate_limit_bytes_per_sec: float,
    ) -> bool:
        session = self._sessions.get(volume.source_volume_id)
        if session is None:
            volume.error = "缺少增量会话，无法完成最终切换"
            return False
        ok = self.ceph_utils.finalize_incremental_volume(
            session,
            on_snapshot=lambda name: self._record_snapshot(volume, name),
            on_removed=lambda name: self._mark_snapshot_removed(volume, name),
            on_phase=lambda label, keep=False: self._set_volume_progress_label(
                volume, label, keep
            ),
            progress_cb=on_progress,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )
        if ok:
            self._sessions.pop(volume.source_volume_id, None)
        return ok

    def _cleanup_volume_snapshots(self, volume: VolumeTask) -> None:
        if not self._job_id or volume.mode != MigrationMode.INCREMENTAL:
            return
        try:
            removed = self.ceph_utils.prune_source_snapshots(
                volume.source_rbd_name, self._job_id, keep=0
            )
        except Exception:  # noqa: BLE001 - 清理失败不改写迁移结论
            logging.exception(
                "[MIGRATION] 迁移快照清理失败 %s", volume.source_rbd_name
            )
            volume.cleanup_status = CLEANUP_PENDING
            self._persist()
            return
        for snap_name in removed:
            self._mark_snapshot_removed(volume, snap_name)
        volume.cleanup_status = CLEANUP_CLEANED
        self._persist()

    def _cleanup_stage_image(self, volume: VolumeTask) -> None:
        """删除本卷的 <target>-mig-stage 暂存镜像；失败不改写迁移结论。"""
        if not volume.target_rbd_name:
            return
        try:
            removed = self.ceph_utils.remove_stage_image(volume.target_rbd_name)
        except Exception:  # noqa: BLE001 - 清理失败不改写迁移结论
            logging.exception(
                "[MIGRATION] 暂存镜像清理失败 %s", volume.target_rbd_name
            )
            return
        if removed:
            logging.info(
                "[MIGRATION] 已清理暂存镜像 %s%s",
                volume.target_rbd_name,
                "-mig-stage",
            )

    def _set_volume_progress_label(
        self, volume: VolumeTask, label: str, keep_percent: bool = False
    ) -> None:
        """切换本轮拷贝的进度标签（基线全量/增量 N/M/末轮增量）。

        `keep_percent=True` 用于"100% 之后"的收尾阶段（布局校验、补建基线
        快照、换入）：数据已经拷完，把进度条打回 0 会让人以为重来了。
        """
        if volume.progress_label == label:
            # 同一阶段被重复播报（例如基像由编排层与 ceph 层各声明一次）时
            # 不再清零进度，避免进度条无意义地回跳。
            return
        volume.progress_label = label
        if not keep_percent:
            volume.progress_percent = 0.0
            volume.throughput_mb_s = None
        self._persist()

    def _cleanup_failed_vm(self, vm: VmTask) -> None:
        """VM 级兜底清理：取消/停机/异常不会走卷级 on_failure，避免残留。

        `_check_stop()` 在取消或停机时直接抛异常，此时已完成预拷贝但尚未换入的
        卷会留下 `-mig-stage` 暂存镜像和源端 `mig-*` 快照，必须在这里补清。
        """
        for volume in vm.volumes:
            if volume.status == VolumeStatus.SUCCESS:
                continue
            self._cleanup_volume_snapshots(volume)
            self._cleanup_stage_image(volume)
            # 卷级工作线程此时已经收尾，把状态落到终态：否则卷会永远停在
            # `copying`，页面显示与真实情况不符，GC 也无法判断它是否还在写。
            if volume.status != VolumeStatus.FAILED:
                volume.status = VolumeStatus.FAILED
                volume.error = volume.error or "VM 失败，已清理暂存镜像与迁移快照"


class FullVolumeMover:
    """全量功能项：一次性 export/import，不创建任何迁移快照。"""

    mode = MigrationMode.FULL

    def __init__(self, manager: MigrationManager):
        self.manager = manager

    def prepare(self, vm: VmTask, options: dict[str, Any]) -> None:
        """全量路径无预拷贝。"""
        return None

    def finish(self, volume: VolumeTask, on_progress, rate_limit_bytes_per_sec):
        return self.manager.ceph_utils.replace_rbd_data(
            volume.source_rbd_name,
            volume.target_rbd_name,
            progress_cb=on_progress,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            on_phase=lambda label, keep=False: (
                self.manager._set_volume_progress_label(volume, label, keep)
            ),
        )

    def on_failure(self, volume: VolumeTask) -> None:
        """失败清理：暂存镜像里只有半份数据，必须删掉，否则永久残留。"""
        self.manager._cleanup_stage_image(volume)


class IncrementalVolumeMover(FullVolumeMover):
    """增量功能项：预拷贝 + 末轮 diff + 换入 + 快照清理。"""

    mode = MigrationMode.INCREMENTAL

    def prepare(self, vm: VmTask, options: dict[str, Any]) -> None:
        self.manager._precopy_volumes(vm, options)

    def finish(self, volume: VolumeTask, on_progress, rate_limit_bytes_per_sec):
        return self.manager._finalize_incremental_volume(
            volume, on_progress, rate_limit_bytes_per_sec
        )

    def on_failure(self, volume: VolumeTask) -> None:
        self.manager._cleanup_volume_snapshots(volume)
        self.manager._cleanup_stage_image(volume)

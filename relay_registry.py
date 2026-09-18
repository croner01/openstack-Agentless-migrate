"""agent 注册表：会话、心跳、任务队列与取消标记。

本模块只维护内存状态，不落盘；持久化由 relay_ledger 负责。
"""
from __future__ import annotations

import functools
import logging
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import RLock
from typing import Any

SUPPORTED_AGENT_VERSIONS = {"1.0.0", "1.1.0"}

#: 任务结果缓存的保留时长：orchestrator 完成拷贝后会反复读 task_result()，
#: 因此不能在 complete_task 时就删；但长跑进程里结果只增不减会吃内存，
#: 超过这个时长的终态结果肯定已被消费，可以安全回收。
RESULT_TTL_SECONDS = 86400.0
#: 触发 TTL 回收的阈值，避免每次上报都遍历整个结果表。
RESULT_PRUNE_THRESHOLD = 512


def _locked(method):
    """把方法体放进 ``self._lock``（RLock，可重入，方法间互相调用安全）。"""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


@dataclass
class AgentRecord:
    agent_id: str
    job_id: str
    role: str
    name: str
    version: str
    address: str
    session_id: str
    #: 常驻节点的 node_id（清单主键）。老版本 agent 不上报，留空时只能按名字匹配。
    node_id: str = ""
    state: str = "ready"
    last_heartbeat: float = 0.0
    cancel_requested: bool = False
    current_task_id: str = ""
    data_address: str = ""
    data_port: int = 9200
    ssh_public_key: str = ""
    slots_total: int = 1
    running_tasks: list[str] = field(default_factory=list)
    cancel_tasks: set[str] = field(default_factory=set)


class RelayState:
    """中转机会话、任务队列与取消标记的内存状态。

    这里的每个方法都会被多个线程调用：Flask 的请求线程（注册/心跳/领任务/
    上报）、作业 worker 线程（入队/取结果/取消）以及巡检线程。队列的
    "遍历 → 命中 → remove" 是典型的 check-then-act，两个 agent 同时轮询
    同一个角色的队列时会有一个 remove 抛 ValueError（表现为 agent 收到 500）。
    因此所有读写都在一把可重入锁内完成；锁内不做 IO、不回调外部代码。
    """

    def __init__(
        self,
        *,
        secret: bytes,
        heartbeat_interval: int = 10,
        heartbeat_timeout: int = 30,
    ):
        self.secret = secret
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._lock = RLock()
        self._agents: dict[str, AgentRecord] = {}
        self._sessions: dict[str, str] = {}
        self._queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self._tasks: dict[str, dict[str, Any]] = {}
        self._ready_tasks: set[str] = set()
        self._results: dict[str, dict[str, Any]] = {}
        self._result_at: dict[str, float] = {}

    def now(self) -> float:
        return time.time()

    @_locked
    def register(
        self,
        *,
        job_id: str,
        role: str,
        name: str,
        version: str,
        address: str,
        now: float,
        data_address: str = "",
        data_port: int = 9200,
        ssh_public_key: str = "",
        slots_total: int = 1,
        node_id: str = "",
    ) -> AgentRecord:
        if version not in SUPPORTED_AGENT_VERSIONS:
            raise ValueError(f"unsupported agent version: {version}")
        # 一个 name 只允许有一条活会话：agent 进程重启/重新注册后，旧会话的
        # last_heartbeat 会永远停在过去，而巡检是按名字把库存记录标 unhealthy 的
        # （app.sweep_relay_resources），留着它就会把同名那台在跑的中转机判死。
        self._supersede_locked(name)
        agent = AgentRecord(
            agent_id=uuid.uuid4().hex,
            job_id=job_id,
            role=role,
            name=name,
            version=version,
            address=address,
            session_id=uuid.uuid4().hex,
            node_id=node_id,
            last_heartbeat=now,
            data_address=data_address,
            data_port=int(data_port or 9200),
            ssh_public_key=ssh_public_key,
            slots_total=max(int(slots_total or 1), 1),
        )
        self._agents[agent.agent_id] = agent
        self._sessions[agent.session_id] = agent.agent_id
        return agent

    @_locked
    def by_session(self, session_id: str) -> AgentRecord | None:
        agent_id = self._sessions.get(session_id)
        return self._agents.get(agent_id) if agent_id else None

    @_locked
    def by_id(self, agent_id: str) -> AgentRecord | None:
        return self._agents.get(agent_id)

    @_locked
    def find_by_name(self, name: str) -> AgentRecord | None:
        latest: AgentRecord | None = None
        for agent in self._agents.values():
            if agent.name == name:
                latest = agent
        return latest

    def _supersede_locked(self, name: str) -> list[str]:
        """作废同名旧会话；调用方必须持有 self._lock。返回被清掉的 agent_id。"""
        dropped: list[str] = []
        for agent_id, agent in list(self._agents.items()):
            if agent.name != name:
                continue
            self._sessions.pop(agent.session_id, None)
            self._agents.pop(agent_id, None)
            dropped.append(agent_id)
        if dropped:
            logging.warning(
                "[MIGRATION] 中转机 %s 重新注册，作废 %s 条旧会话", name, len(dropped)
            )
        return dropped

    @_locked
    def drop_by_name(self, name: str) -> None:
        """重建中转机时清掉旧 agent 记录，避免同名旧会话被误用。"""
        self._supersede_locked(name)

    @_locked
    def prune_stale(self, *, now: float, max_age: float) -> list[str]:
        """清掉长时间没心跳且没在跑任务的会话。

        正常情况下同名旧会话已在注册时作废，这里只是兜底：避免 agent 进程
        消失后记录只增不减，也避免巡检反复扫到陈年僵尸。
        """
        if max_age <= 0:
            return []
        dropped: list[str] = []
        for agent_id, agent in list(self._agents.items()):
            if agent.running_tasks:
                continue
            if now - float(agent.last_heartbeat or 0.0) < max_age:
                continue
            self._sessions.pop(agent.session_id, None)
            self._agents.pop(agent_id, None)
            dropped.append(agent_id)
        return dropped

    @_locked
    def agents(self) -> list[AgentRecord]:
        return list(self._agents.values())

    @_locked
    def mark_task_ready(self, task_id: str) -> None:
        self._ready_tasks.add(task_id)

    @_locked
    def task_ready(self, task_id: str) -> bool:
        return task_id in self._ready_tasks

    @_locked
    def heartbeat(self, session_id: str, *, now: float) -> AgentRecord | None:
        agent = self.by_session(session_id)
        if agent is None:
            return None
        agent.last_heartbeat = now
        if agent.state == "unhealthy":
            agent.state = "busy" if agent.running_tasks else "ready"
        return agent

    @_locked
    def sweep(self, *, now: float) -> list[str]:
        changed: list[str] = []
        for agent in self._agents.values():
            if agent.state == "unhealthy":
                continue
            if now - agent.last_heartbeat > self.heartbeat_timeout:
                agent.state = "unhealthy"
                changed.append(agent.agent_id)
        return changed

    @_locked
    def enqueue(self, task: dict[str, Any]) -> None:
        self._tasks[task["task_id"]] = task
        self._queues[task["role"]].append(task)

    @_locked
    def task(self, task_id: str) -> dict[str, Any] | None:
        return self._tasks.get(task_id)

    @_locked
    def dispatch(self, session_id: str) -> dict[str, Any] | None:
        agent = self.by_session(session_id)
        if agent is None or agent.state not in {"ready", "busy"}:
            return None
        if len(agent.running_tasks) >= max(int(agent.slots_total or 1), 1):
            return None
        queue = self._queues.get(agent.role)
        if not queue:
            return None
        # 卷挂在某一台机器上，任务绑定 agent_id 时只能由该 agent 领取。
        for task in list(queue):
            if not self._task_matches_agent(task, agent):
                continue
            queue.remove(task)
            agent.state = "busy"
            agent.current_task_id = task["task_id"]
            agent.running_tasks.append(task["task_id"])
            return task
        return None

    @staticmethod
    def _task_matches_agent(task: dict[str, Any], agent: AgentRecord) -> bool:
        """任务与 agent 的绑定规则。

        卷挂在哪台机器上，任务就只能由那台机器领取。绑定时优先用节点名：
        agent_id 是每次注册新生成的，而节点名在重建/重注册后保持不变，
        按 id 绑定会让任务在 agent 重注册后无人领取。
        """
        pinned_id = task.get("agent_id")
        if pinned_id and pinned_id != agent.agent_id:
            return False
        pinned_name = task.get("agent_name")
        if pinned_name and pinned_name != agent.name:
            return False
        pinned_job = task.get("job_id")
        if pinned_job and agent.job_id and pinned_job != agent.job_id:
            return False
        return True

    @_locked
    def record_result(
        self,
        task_id: str,
        status: str,
        copied_bytes: int,
        *,
        digest: str = "",
        error: str = "",
    ) -> None:
        self._results[task_id] = {
            "status": status,
            "copied_bytes": copied_bytes,
            "digest": digest,
            "error": error,
        }
        self._result_at[task_id] = time.time()
        self._prune_results_locked()

    def _prune_results_locked(self) -> None:
        """调用方必须持有 self._lock：回收超过 TTL 的终态结果。"""
        if len(self._result_at) <= RESULT_PRUNE_THRESHOLD:
            return
        cutoff = time.time() - RESULT_TTL_SECONDS
        stale = [key for key, at in self._result_at.items() if at < cutoff]
        for key in stale:
            self._result_at.pop(key, None)
            self._results.pop(key, None)

    @_locked
    def forget_job(self, job_id: str) -> int:
        """作业结束后回收该作业的任务、就绪标记与结果，避免内存只增不减。"""
        if not job_id:
            return 0
        task_ids = {
            task_id
            for task_id, task in self._tasks.items()
            if task.get("job_id") == job_id
        }
        for task_id in task_ids:
            self._tasks.pop(task_id, None)
            self._ready_tasks.discard(task_id)
            self._results.pop(task_id, None)
            self._result_at.pop(task_id, None)
        for agent in self._agents.values():
            if not task_ids:
                continue
            remaining = [item for item in agent.running_tasks if item not in task_ids]
            if len(remaining) != len(agent.running_tasks):
                agent.running_tasks = remaining
                agent.current_task_id = (
                    agent.running_tasks[-1] if agent.running_tasks else ""
                )
                agent.state = "busy" if agent.running_tasks else "ready"
            agent.cancel_tasks.difference_update(task_ids)
        return len(task_ids)

    @_locked
    def task_result(self, task_id: str) -> dict[str, Any] | None:
        return self._results.get(task_id)

    @_locked
    def tasks_for_job(self, job_id: str) -> list[dict[str, Any]]:
        """返回该作业下已下发的任务，供取消时按任务下发取消指令。"""
        return [
            task for task in self._tasks.values() if task.get("job_id") == job_id
        ]

    @_locked
    def complete_task(self, session_id: str, task_id: str = "") -> None:
        """完成任务释放槽位；不传 task_id 时清空该 agent 的全部在途任务。"""
        agent = self.by_session(session_id)
        if agent is None:
            return
        if task_id:
            agent.running_tasks = [
                item for item in agent.running_tasks if item != task_id
            ]
            agent.cancel_tasks.discard(task_id)
            # 就绪标记只在"等 agent 开始监听"期间有用，任务完成后必须清掉，
            # 否则 _ready_tasks 会随历史任务无限累积。
            self._ready_tasks.discard(task_id)
        else:
            for item in agent.running_tasks:
                self._ready_tasks.discard(item)
            agent.running_tasks = []
        agent.current_task_id = agent.running_tasks[-1] if agent.running_tasks else ""
        agent.state = "busy" if agent.running_tasks else "ready"

    @_locked
    def request_cancel(self, task_id: str) -> None:
        """按任务取消，避免取消一个作业误伤同机其他作业的拷贝。"""
        for agent in self._agents.values():
            if task_id in agent.running_tasks:
                agent.cancel_tasks.add(task_id)
        # 兼容旧调用：传入 session_id 时取消该 agent 上的全部在途任务。
        agent = self.by_session(task_id)
        if agent is not None:
            agent.cancel_tasks.update(agent.running_tasks)

    @_locked
    def cancels_for(self, session_id: str) -> list[str]:
        agent = self.by_session(session_id)
        return sorted(agent.cancel_tasks) if agent else []

    @_locked
    def has_cancel(self, session_id: str, task_id: str = "") -> bool:
        agent = self.by_session(session_id)
        if agent is None:
            return False
        if task_id:
            return task_id in agent.cancel_tasks
        return bool(agent.cancel_tasks)

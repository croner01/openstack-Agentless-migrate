"""Reproduce the exact BFV create path used by a migration run.

Purpose: when a real migration fails with VolumeSizeExceedsAvailableQuota,
run this inside the deployment with the same credentials to pinpoint whether
the failure comes from the connection scope, the data volumes, or the BFV
server (boot volume) request.

Usage:
    python repro_create.py \
      --auth-url http://keystone:5000/v3 \
      --username admin --password <pass> \
      --user-domain-name Default \
      --project-id <target project UUID> \
      --image <target image id> --flavor <target flavor id> \
      --network <target network id> --subnet <target subnet id> \
      --az nova --size 10 --name mig-repro
"""

import argparse
import json

from openstack_utils import OpenStackUtils


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--auth-url", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--user-domain-name", default="Default")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--project-domain-name", default="Default")
    parser.add_argument("--image", required=True)
    parser.add_argument("--flavor", required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--subnet", required=True)
    parser.add_argument("--az", default="nova")
    parser.add_argument("--size", type=int, default=10)
    parser.add_argument("--name", default="mig-repro")
    parser.add_argument(
        "--volume-type",
        default=None,
        help="volume type name/id; MUST match the migration page 目标卷类型",
    )
    parser.add_argument("--no-server", action="store_true", help="stop after volumes")
    args = parser.parse_args()

    auth_args = {
        "auth_url": args.auth_url,
        "username": args.username,
        "password": args.password,
        "user_domain_name": args.user_domain_name,
        "project_id": args.project_id,
    }
    if args.project_domain_name:
        auth_args["project_domain_name"] = args.project_domain_name

    os_utils = OpenStackUtils(auth_args)
    print("== 连接作用域信息 ==")
    print(json.dumps(os_utils.diagnostic_info(), ensure_ascii=False, indent=2, default=str))

    # Step 1: blank data volume (same call migration makes for data disks)
    data_volume = os_utils.create_blank_volume(
        name=f"{args.name}-data",
        size=args.size,
        volume_type=args.volume_type,
        availability_zone=args.az,
    )
    print(f"数据卷创建成功 volume_id={data_volume.id}")
    os_utils.wait_volume_status(data_volume.id)
    print("数据卷已 available，开始复现 BFV 建 VM")

    if args.no_server:
        print("已按 --no-server 停止，未创建 VM。可手动清理：", data_volume.id)
        return

    # Step 2: port in the same project/subnet as migration uses
    port = os_utils.create_port_with_fixed_ip(
        network_id=args.network,
        subnet_id=args.subnet,
        fixed_ip=None,
    )
    print(f"端口创建成功 port_id={port.id}")

    # Step 3: BFV VM -- the boot volume is created by Nova here and is where
    #         VolumeSizeExceedsAvailableQuota is reported.
    server = os_utils.create_bfv_server(
        name=args.name,
        image_id=args.image,
        flavor_id=args.flavor,
        port_ids=[port.id],
        boot_volume_size=args.size,
        data_volume_ids=[data_volume.id],
        availability_zone=args.az,
        admin_password="P@ssw0rd",
        volume_type=args.volume_type,
    )
    print(f"BFV VM 创建成功 server_id={server.id}")
    print(
        "复现完成。请保留 server/volume/port 供人工排查：\n"
        f"server={server.id}\nvolume={data_volume.id}\nport={port.id}"
    )


if __name__ == "__main__":
    main()

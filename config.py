import os
import secrets

# 配置信息
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

#: 迁移目标 VM 的管理员密码。生产环境请显式设置 MIGRATION_DEFAULT_VM_PASSWORD；
#: 未设置时按进程随机生成，避免"全网同一弱口令"被直接猜中。
default_vm_pass = os.environ.get("MIGRATION_DEFAULT_VM_PASSWORD") or secrets.token_urlsafe(16)

# 日志配置：必须落在 UPLOAD_FOLDER（hostPath 持久化）下，否则 Pod 重启后
# 历史日志随容器一起消失，出问题只能靠作业状态反推。
LOG_FILE = os.path.join(UPLOAD_FOLDER, 'vm_batch_migration.log')

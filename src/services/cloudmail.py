"""
CloudMail 邮箱服务实现
当前复用 Freemail 的自部署 Worker 接口协议，提供独立服务类型入口。
"""

from .freemail import FreemailService
from .base import EmailServiceType


class CloudMailService(FreemailService):
    """CloudMail 邮箱服务"""

    def __init__(self, config=None, name=None):
        super().__init__(config=config, name=name)
        self.service_type = EmailServiceType.CLOUDMAIL

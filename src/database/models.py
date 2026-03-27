"""
SQLAlchemy ORM 模型定义
"""

from datetime import datetime
from typing import Optional, Dict, Any
import json
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import relationship

Base = declarative_base()

class JSONEncodedDict(TypeDecorator):
    """JSON 编码字典类型"""
    impl = Text

    def process_bind_param(self, value: Optional[Dict[str, Any]], dialect):
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False)

    def process_result_value(self, value: Optional[str], dialect):
        if value is None:
            return None
        return json.loads(value)


class Account(Base):
    """已注册账号表"""
    __tablename__ = 'accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), nullable=False, unique=True, index=True)
    password = Column(String(255))  # 注册密码（明文存储）
    access_token = Column(Text)
    refresh_token = Column(Text)
    id_token = Column(Text)
    session_token = Column(Text)  # 会话令牌（优先刷新方式）
    client_id = Column(String(255))  # OAuth Client ID
    account_id = Column(String(255))
    workspace_id = Column(String(255))
    email_service = Column(String(50), nullable=False)  # 'tempmail', 'outlook', 'moe_mail'
    email_service_id = Column(String(255))  # 邮箱服务中的ID
    proxy_used = Column(String(255))
    registered_at = Column(DateTime, default=datetime.utcnow)
    last_refresh = Column(DateTime)  # 最后刷新时间
    expires_at = Column(DateTime)  # Token 过期时间
    status = Column(String(20), default='active')  # 'active', 'expired', 'banned', 'failed'
    extra_data = Column(JSONEncodedDict)  # 额外信息存储
    cpa_uploaded = Column(Boolean, default=False)  # 是否已上传到 CPA
    cpa_uploaded_at = Column(DateTime)  # 上传时间
    source = Column(String(20), default='register')  # 'register' 或 'login'，区分账号来源
    platform_source = Column(String(50))  # 账号来源平台，如 cloudmail
    last_upload_target = Column(String(20))  # 最近成功上传目标，如 newApi
    subscription_type = Column(String(20))  # None / 'plus' / 'team'
    subscription_at = Column(DateTime)  # 订阅开通时间
    cookies = Column(Text)  # 完整 cookie 字符串，用于支付请求
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'email': self.email,
            'password': self.password,
            'client_id': self.client_id,
            'email_service': self.email_service,
            'account_id': self.account_id,
            'workspace_id': self.workspace_id,
            'registered_at': self.registered_at.isoformat() if self.registered_at else None,
            'last_refresh': self.last_refresh.isoformat() if self.last_refresh else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'status': self.status,
            'proxy_used': self.proxy_used,
            'cpa_uploaded': self.cpa_uploaded,
            'cpa_uploaded_at': self.cpa_uploaded_at.isoformat() if self.cpa_uploaded_at else None,
            'source': self.source,
            'platform_source': self.platform_source,
            'last_upload_target': self.last_upload_target,
            'subscription_type': self.subscription_type,
            'subscription_at': self.subscription_at.isoformat() if self.subscription_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class EmailService(Base):
    """邮箱服务配置表"""
    __tablename__ = 'email_services'

    id = Column(Integer, primary_key=True, autoincrement=True)
    service_type = Column(String(50), nullable=False)  # 'outlook', 'moe_mail'
    name = Column(String(100), nullable=False)
    config = Column(JSONEncodedDict, nullable=False)  # 服务配置（加密存储）
    enabled = Column(Boolean, default=True)
    priority = Column(Integer, default=0)  # 使用优先级
    last_used = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class RegistrationTask(Base):
    """注册任务表"""
    __tablename__ = 'registration_tasks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_uuid = Column(String(36), unique=True, nullable=False, index=True)  # 任务唯一标识
    status = Column(String(20), default='pending')  # 'pending', 'running', 'completed', 'failed', 'cancelled'
    email_service_id = Column(Integer, ForeignKey('email_services.id'), index=True)  # 使用的邮箱服务
    proxy = Column(String(255))  # 使用的代理
    logs = Column(Text)  # 注册过程日志
    result = Column(JSONEncodedDict)  # 注册结果
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    # 关系
    email_service = relationship('EmailService')


class Setting(Base):
    """系统设置表"""
    __tablename__ = 'settings'

    key = Column(String(100), primary_key=True)
    value = Column(Text)
    description = Column(Text)
    category = Column(String(50), default='general')  # 'general', 'email', 'proxy', 'openai'
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CpaService(Base):
    """CPA 服务配置表"""
    __tablename__ = 'cpa_services'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)  # 服务名称
    api_url = Column(String(500), nullable=False)  # API URL
    api_token = Column(Text, nullable=False)  # API Token
    enabled = Column(Boolean, default=True)
    priority = Column(Integer, default=0)  # 优先级
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Sub2ApiService(Base):
    """Sub2API 服务配置表"""
    __tablename__ = 'sub2api_services'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)  # 服务名称
    api_url = Column(String(500), nullable=False)  # API URL (host)
    api_key = Column(Text, nullable=False)  # x-api-key
    target_type = Column(String(20), nullable=False, default='sub2api')  # 'sub2api' or 'newApi'
    enabled = Column(Boolean, default=True)
    priority = Column(Integer, default=0)  # 优先级
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TeamManagerService(Base):
    """Team Manager 服务配置表"""
    __tablename__ = 'tm_services'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)  # 服务名称
    api_url = Column(String(500), nullable=False)  # API URL
    api_key = Column(Text, nullable=False)  # X-API-Key
    enabled = Column(Boolean, default=True)
    priority = Column(Integer, default=0)  # 优先级
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Proxy(Base):
    """代理列表表"""
    __tablename__ = 'proxies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)  # 代理名称
    type = Column(String(20), nullable=False, default='http')  # http, socks5
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    username = Column(String(100))
    password = Column(String(255))
    enabled = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)  # 是否为默认代理
    priority = Column(Integer, default=0)  # 优先级（保留字段）
    last_used = Column(DateTime)  # 最后使用时间
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self, include_password: bool = False) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'enabled': self.enabled,
            'is_default': self.is_default or False,
            'priority': self.priority,
            'last_used': self.last_used.isoformat() if self.last_used else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
        if include_password:
            result['password'] = self.password
        else:
            result['has_password'] = bool(self.password)
        return result

    @property
    def proxy_url(self) -> str:
        """获取完整的代理 URL"""
        if self.type == "http":
            scheme = "http"
        elif self.type == "socks5":
            scheme = "socks5"
        else:
            scheme = self.type

        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"

        return f"{scheme}://{auth}{self.host}:{self.port}"


class CLIProxyAPIEnvironment(Base):
    """CLIProxyAPI 远端环境配置表"""
    __tablename__ = 'cliproxy_environments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True, index=True)
    base_url = Column(String(500), nullable=False)
    _token_encrypted = Column("token_encrypted", Text)
    target_type = Column(String(50), nullable=False)
    provider = Column(String(50), nullable=False)
    provider_scope = Column(String(100))
    target_scope = Column(String(100))
    scope_rules_json = Column(JSONEncodedDict)
    enabled = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    notes = Column(Text)
    last_test_status = Column(String(20), default='unknown')
    last_test_latency_ms = Column(Integer)
    last_test_error = Column(Text)
    last_scanned_at = Column(DateTime)
    last_maintained_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @property
    def has_token(self) -> bool:
        return bool(self._token_encrypted)

    @property
    def token_encrypted(self) -> str:
        return self._token_encrypted or ""

    @token_encrypted.setter
    def token_encrypted(self, value: Optional[str]) -> None:
        from ..core.cliproxy.secrets import encrypt_cliproxy_token

        if not value:
            self._token_encrypted = ""
            return

        self._token_encrypted = encrypt_cliproxy_token(value)

    def set_encrypted_token(self, token_encrypted: Optional[str]) -> None:
        from ..core.cliproxy.secrets import decrypt_cliproxy_token

        if not token_encrypted:
            self._token_encrypted = ""
            return

        decrypt_cliproxy_token(token_encrypted)
        self._token_encrypted = token_encrypted

    def to_summary_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'base_url': self.base_url,
            'target_type': self.target_type,
            'provider': self.provider,
            'provider_scope': self.provider_scope,
            'target_scope': self.target_scope,
            'enabled': self.enabled,
            'is_default': self.is_default or False,
            'has_token': self.has_token,
            'last_test_status': self.last_test_status,
            'last_test_latency_ms': self.last_test_latency_ms,
            'last_test_error': self.last_test_error,
            'last_scanned_at': self.last_scanned_at.isoformat() if self.last_scanned_at else None,
            'last_maintained_at': self.last_maintained_at.isoformat() if self.last_maintained_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    @staticmethod
    def maintenance_contract_v1() -> Dict[str, Any]:
        return {
            'refill': {
                'state': 'reserved',
                'enabled': False,
                'version': 'v1',
            }
        }

    def to_detail_dict(self) -> Dict[str, Any]:
        from ..core.cliproxy.secrets import mask_cliproxy_token

        payload = self.to_summary_dict()
        payload['scope_rules_json'] = self.scope_rules_json
        payload['notes'] = self.notes
        payload['maintenance_contract'] = self.maintenance_contract_v1()
        payload['token_masked'] = mask_cliproxy_token(self.token_encrypted) if self.token_encrypted else ''
        return payload

    def set_token(self, token: str) -> None:
        from ..core.cliproxy.secrets import encrypt_cliproxy_token

        if not token:
            self._token_encrypted = ""
            return

        self.token_encrypted = token

    def get_token(self) -> str:
        from ..core.cliproxy.secrets import decrypt_cliproxy_token

        return decrypt_cliproxy_token(self.token_encrypted)


class RemoteAuthInventory(Base):
    """远端认证库存快照表"""
    __tablename__ = 'remote_auth_inventory'
    __table_args__ = (
        UniqueConstraint('environment_id', 'remote_file_id', name='uq_remote_auth_inventory_env_file'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    environment_id = Column(Integer, ForeignKey('cliproxy_environments.id'), nullable=False, index=True)
    remote_file_id = Column(String(255), nullable=False)
    email = Column(String(255))
    remote_account_id = Column(String(255))
    local_account_id = Column(Integer, ForeignKey('accounts.id'), index=True)
    payload_json = Column(JSONEncodedDict)
    last_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_probed_at = Column(DateTime)
    sync_state = Column(String(50), default='unlinked')
    probe_status = Column(String(50), default='unknown')
    disable_source = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    environment = relationship('CLIProxyAPIEnvironment')
    account = relationship('Account')


class MaintenanceRun(Base):
    """维护运行记录表"""
    __tablename__ = 'maintenance_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    environment_id = Column(Integer, ForeignKey('cliproxy_environments.id'), index=True)
    run_type = Column(String(32), nullable=False)
    status = Column(String(20), nullable=False, default='pending')
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    summary_json = Column(JSONEncodedDict)
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    environment = relationship('CLIProxyAPIEnvironment')


class MaintenanceActionLog(Base):
    """维护动作审计明细表"""
    __tablename__ = 'maintenance_action_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey('maintenance_runs.id'), nullable=False, index=True)
    environment_id = Column(Integer, ForeignKey('cliproxy_environments.id'), index=True)
    action_type = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False, default='pending')
    remote_file_id = Column(String(255))
    message = Column(Text)
    details_json = Column(JSONEncodedDict)
    created_at = Column(DateTime, default=datetime.utcnow)

    run = relationship('MaintenanceRun')
    environment = relationship('CLIProxyAPIEnvironment')


class AuditLog(Base):
    """审计日志表"""
    __tablename__ = 'audit_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    environment_id = Column(Integer, ForeignKey('cliproxy_environments.id'), index=True)
    run_id = Column(Integer, ForeignKey('maintenance_runs.id'), index=True)
    event_type = Column(String(50), nullable=False)
    actor = Column(String(100), nullable=False, default='system')
    message = Column(Text)
    details_json = Column(JSONEncodedDict)
    created_at = Column(DateTime, default=datetime.utcnow)

    environment = relationship('CLIProxyAPIEnvironment')
    run = relationship('MaintenanceRun')

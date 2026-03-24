"""
Temp-Mail 邮箱服务实现
基于自部署 Cloudflare Worker 临时邮箱服务
接口文档参见 plan/temp-mail.md
"""

import re
import time
import json
import logging
from email import message_from_string
from email.header import decode_header, make_header
from email.message import Message
from email.policy import default as email_policy
from html import unescape
from typing import Optional, Dict, Any, List

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from ..core.http_client import HTTPClient, RequestConfig
from ..core.utils import calculate_sha256
from ..config.constants import OTP_CODE_PATTERN


logger = logging.getLogger(__name__)


class TempMailService(BaseEmailService):
    """
    Temp-Mail 邮箱服务
    基于自部署 Cloudflare Worker 的临时邮箱，admin 创建地址 + 地址密码登录收件箱
    不走代理，不使用 requests 库
    """

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        """
        初始化 TempMail 服务

        Args:
            config: 配置字典，支持以下键:
                - base_url: Worker 域名地址，如 https://mail.example.com (必需)
                - admin_password: Admin 密码，对应 x-admin-auth header (创建邮箱必需)
                - domain: 邮箱域名，如 example.com (必需)
                - site_password: 站点全局密码，对应 x-custom-auth header (可选)
                - enable_prefix: 是否启用前缀，默认 True
                - timeout: 请求超时时间，默认 30
                - max_retries: 最大重试次数，默认 3
            name: 服务名称
        """
        super().__init__(EmailServiceType.TEMP_MAIL, name)

        required_keys = ["base_url", "admin_password", "domain"]
        missing_keys = [key for key in required_keys if not (config or {}).get(key)]
        if missing_keys:
            raise ValueError(f"缺少必需配置: {missing_keys}")

        default_config = {
            "enable_prefix": True,
            "timeout": 30,
            "max_retries": 3,
        }
        self.config = {**default_config, **(config or {})}

        # 不走代理，proxy_url=None
        http_config = RequestConfig(
            timeout=self.config["timeout"],
            max_retries=self.config["max_retries"],
        )
        self.http_client = HTTPClient(proxy_url=None, config=http_config)

        # 邮箱缓存：email -> {jwt, address, password}
        self._email_cache: Dict[str, Dict[str, Any]] = {}

    def _decode_mime_header(self, value: str) -> str:
        """解码 MIME 头，兼容 RFC 2047 编码主题。"""
        if not value:
            return ""
        try:
            return str(make_header(decode_header(value)))
        except Exception:
            return value

    def _extract_body_from_message(self, message: Message) -> str:
        """从 MIME 邮件对象中提取可读正文。"""
        parts: List[str] = []

        if message.is_multipart():
            for part in message.walk():
                if part.get_content_maintype() == "multipart":
                    continue

                content_type = (part.get_content_type() or "").lower()
                if content_type not in ("text/plain", "text/html"):
                    continue

                try:
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="replace") if payload else ""
                except Exception:
                    try:
                        text = part.get_content()
                    except Exception:
                        text = ""

                if content_type == "text/html":
                    text = re.sub(r"<[^>]+>", " ", text)
                parts.append(text)
        else:
            try:
                payload = message.get_payload(decode=True)
                charset = message.get_content_charset() or "utf-8"
                body = payload.decode(charset, errors="replace") if payload else ""
            except Exception:
                try:
                    body = message.get_content()
                except Exception:
                    body = str(message.get_payload() or "")

            if "html" in (message.get_content_type() or "").lower():
                body = re.sub(r"<[^>]+>", " ", body)
            parts.append(body)

        return unescape("\n".join(part for part in parts if part).strip())

    def _extract_mail_fields(self, mail: Dict[str, Any]) -> Dict[str, str]:
        """统一提取邮件字段，兼容 raw MIME 和不同 Worker 返回格式。"""
        sender = str(
            mail.get("source")
            or mail.get("from")
            or mail.get("from_address")
            or mail.get("fromAddress")
            or ""
        ).strip()
        subject = str(mail.get("subject") or mail.get("title") or "").strip()
        body_text = str(
            mail.get("text")
            or mail.get("body")
            or mail.get("content")
            or mail.get("html")
            or ""
        ).strip()
        raw = str(mail.get("raw") or "").strip()

        if raw:
            try:
                message = message_from_string(raw, policy=email_policy)
                sender = sender or self._decode_mime_header(message.get("From", ""))
                subject = subject or self._decode_mime_header(message.get("Subject", ""))
                parsed_body = self._extract_body_from_message(message)
                if parsed_body:
                    body_text = f"{body_text}\n{parsed_body}".strip() if body_text else parsed_body
            except Exception as e:
                logger.debug(f"解析 TempMail raw 邮件失败: {e}")
                body_text = f"{body_text}\n{raw}".strip() if body_text else raw

        body_text = unescape(re.sub(r"<[^>]+>", " ", body_text))
        return {
            "sender": sender,
            "subject": subject,
            "body": body_text,
            "raw": raw,
        }

    def _default_headers(self) -> Dict[str, str]:
        """构造基础请求头"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        site_password = str(self.config.get("site_password") or "").strip()
        if site_password:
            headers["x-custom-auth"] = site_password
        return headers

    def _admin_headers(self) -> Dict[str, str]:
        headers = self._default_headers()
        headers["x-admin-auth"] = self.config["admin_password"]
        return headers

    def _address_headers(self, jwt: str) -> Dict[str, str]:
        headers = self._default_headers()
        headers["Authorization"] = f"Bearer {jwt}"
        return headers

    def _login_address(self, email: str, password: str) -> str:
        """使用地址密码登录并返回最新 JWT。"""
        if not email or not password:
            raise EmailServiceError("缺少地址密码，无法登录 TempMail 邮箱")

        response = self._make_request(
            "POST",
            "/api/address_login",
            json={
                "email": email,
                "password": calculate_sha256(password),
            },
        )
        jwt = str(response.get("jwt") or "").strip()
        if not jwt:
            raise EmailServiceError(f"TempMail 地址登录返回数据不完整: {response}")
        cached = self._email_cache.get(email, {})
        self._email_cache[email] = {
            **cached,
            "email": email,
            "jwt": jwt,
            "address": str(response.get("address") or email).strip() or email,
            "password": password,
        }
        return jwt

    def _make_request(self, method: str, path: str, **kwargs) -> Any:
        """
        发送请求并返回 JSON 数据

        Args:
            method: HTTP 方法
            path: 请求路径（以 / 开头）
            **kwargs: 传递给 http_client.request 的额外参数

        Returns:
            响应 JSON 数据

        Raises:
            EmailServiceError: 请求失败
        """
        base_url = self.config["base_url"].rstrip("/")
        url = f"{base_url}{path}"

        # 合并默认 headers
        kwargs.setdefault("headers", {})
        for k, v in self._default_headers().items():
            kwargs["headers"].setdefault(k, v)

        try:
            response = self.http_client.request(method, url, **kwargs)

            if response.status_code >= 400:
                error_msg = f"请求失败: {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = f"{error_msg} - {error_data}"
                except Exception:
                    error_msg = f"{error_msg} - {response.text[:200]}"
                self.update_status(False, EmailServiceError(error_msg))
                raise EmailServiceError(error_msg)

            try:
                return response.json()
            except json.JSONDecodeError:
                return {"raw_response": response.text}

        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"请求失败: {method} {path} - {e}")

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        通过 admin API 创建临时邮箱

        Returns:
            包含邮箱信息的字典:
            - email: 邮箱地址
            - jwt: 地址级 JWT token
            - password: 地址密码
            - service_id: 同 email（用作标识）
        """
        import random
        import string

        # 生成随机邮箱名
        letters = ''.join(random.choices(string.ascii_lowercase, k=5))
        digits = ''.join(random.choices(string.digits, k=random.randint(1, 3)))
        suffix = ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 3)))
        name = letters + digits + suffix

        domain = self.config["domain"]
        enable_prefix = self.config.get("enable_prefix", True)

        body = {
            "name": name,
            "domain": domain,
        }

        try:
            response = self._make_request(
                "POST",
                "/admin/new_address",
                json=body,
                headers=self._admin_headers(),
            )

            address = response.get("address", "").strip()
            jwt = response.get("jwt", "").strip()
            password = response.get("password", "").strip()

            if not address:
                raise EmailServiceError(f"API 返回数据不完整: {response}")

            email_info = {
                "email": address,
                "jwt": jwt,
                "password": password,
                "service_id": address,
                "id": address,
                "created_at": time.time(),
            }

            # 缓存 jwt，供获取验证码时使用
            self._email_cache[address] = email_info

            logger.info(f"成功创建 TempMail 邮箱: {address}")
            self.update_status(True)
            return email_info

        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"创建邮箱失败: {e}")

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
    ) -> Optional[str]:
        """
        从 TempMail 邮箱获取验证码

        Args:
            email: 邮箱地址
            email_id: 未使用，保留接口兼容
            timeout: 超时时间（秒）
            pattern: 验证码正则
            otp_sent_at: OTP 发送时间戳（暂未使用）

        Returns:
            验证码字符串，超时返回 None
        """
        logger.info(f"正在从 TempMail 邮箱 {email} 获取验证码...")

        start_time = time.time()
        seen_mail_ids: set = set()

        # 优先使用地址级 JWT，401 时使用地址密码重新登录
        cached = self._email_cache.get(email, {})
        jwt = cached.get("jwt")
        password = cached.get("password")

        if not jwt and password:
            try:
                jwt = self._login_address(email, password)
            except Exception as e:
                logger.debug(f"TempMail 地址登录失败: {e}")

        while time.time() - start_time < timeout:
            try:
                if jwt:
                    try:
                        response = self._make_request(
                            "GET",
                            "/api/mails",
                            params={"limit": 20, "offset": 0},
                            headers=self._address_headers(jwt),
                        )
                    except EmailServiceError as e:
                        if "401" in str(e) and password:
                            jwt = self._login_address(email, password)
                            response = self._make_request(
                                "GET",
                                "/api/mails",
                                params={"limit": 20, "offset": 0},
                                headers=self._address_headers(jwt),
                            )
                        else:
                            raise
                else:
                    logger.debug("TempMail 邮箱 %s 缺少可用 JWT，等待下次轮询", email)
                    time.sleep(3)
                    continue

                # /api/mails 返回格式: {"results": [...], "count": N}
                mails = response.get("results", [])
                if not isinstance(mails, list):
                    time.sleep(3)
                    continue

                for mail in mails:
                    mail_id = mail.get("id")
                    if not mail_id or mail_id in seen_mail_ids:
                        continue

                    seen_mail_ids.add(mail_id)

                    parsed = self._extract_mail_fields(mail)
                    sender = parsed["sender"].lower()
                    subject = parsed["subject"]
                    body_text = parsed["body"]
                    raw_text = parsed["raw"]
                    content = f"{sender}\n{subject}\n{body_text}\n{raw_text}".strip()

                    # 只处理 OpenAI 邮件
                    if "openai" not in sender and "openai" not in content.lower():
                        continue

                    match = re.search(pattern, content)
                    if match:
                        code = match.group(1)
                        logger.info(f"从 TempMail 邮箱 {email} 找到验证码: {code}")
                        self.update_status(True)
                        return code

            except Exception as e:
                logger.debug(f"检查 TempMail 邮件时出错: {e}")

            time.sleep(3)

        logger.warning(f"等待 TempMail 验证码超时: {email}")
        return None

    def list_emails(self, limit: int = 100, offset: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """
        列出邮箱

        当前服务不提供全局邮箱列表，只返回本地缓存的邮箱。

        Returns:
            邮箱列表
        """
        try:
            self.update_status(True)
            emails = list(self._email_cache.values())
            return emails[offset: offset + limit]
        except Exception as e:
            logger.warning(f"列出 TempMail 邮箱失败: {e}")
            self.update_status(False, e)
            return list(self._email_cache.values())

    def delete_email(self, email_id: str) -> bool:
        """
        删除邮箱

        Note:
            当前 TempMail admin API 文档未见删除地址接口，这里先从本地缓存移除，
            以满足统一接口并避免服务实例化失败。
        """
        removed = False
        emails_to_delete = []

        for address, info in self._email_cache.items():
            candidate_ids = {
                address,
                info.get("id"),
                info.get("service_id"),
            }
            if email_id in candidate_ids:
                emails_to_delete.append(address)

        for address in emails_to_delete:
            self._email_cache.pop(address, None)
            removed = True

        if removed:
            logger.info(f"已从 TempMail 缓存移除邮箱: {email_id}")
            self.update_status(True)
        else:
            logger.info(f"TempMail 缓存中未找到邮箱: {email_id}")

        return removed

    def check_health(self) -> bool:
        """检查服务健康状态"""
        try:
            self._make_request(
                "GET",
                "/health_check",
            )
            self.update_status(True)
            return True
        except Exception as e:
            logger.warning(f"TempMail 健康检查失败: {e}")
            self.update_status(False, e)
            return False

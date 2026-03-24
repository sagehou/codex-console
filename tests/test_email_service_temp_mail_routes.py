import asyncio

from src.config.constants import EmailServiceType
from src.services.base import EmailServiceFactory
from src.web.routes import email as email_routes


def test_temp_mail_service_registered():
    service_type = EmailServiceType("temp_mail")
    service_class = EmailServiceFactory.get_service_class(service_type)
    assert service_class is not None
    assert service_class.__name__ == "TempMailService"


def test_email_service_types_include_temp_mail_site_password():
    result = asyncio.run(email_routes.get_service_types())
    temp_mail_type = next(item for item in result["types"] if item["value"] == "temp_mail")

    assert temp_mail_type["label"] == "Temp-Mail（自部署）"
    field_names = [field["name"] for field in temp_mail_type["config_fields"]]
    assert "base_url" in field_names
    assert "domain" in field_names
    assert "site_password" in field_names
    assert "admin_password" in field_names


def test_filter_sensitive_config_marks_temp_mail_site_password():
    filtered = email_routes.filter_sensitive_config({
        "base_url": "https://mail.example.com",
        "site_password": "site-secret",
        "domain": "example.com",
    })

    assert filtered["base_url"] == "https://mail.example.com"
    assert filtered["domain"] == "example.com"
    assert filtered["has_site_password"] is True
    assert "site_password" not in filtered

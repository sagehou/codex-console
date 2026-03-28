from pathlib import Path


def test_email_services_template_mentions_tempmail_multi_domain_and_clear_controls():
    template = Path("templates/email_services.html").read_text(encoding="utf-8")

    assert "example.com, example.org" in template
    assert "随机选择一个" in template
    assert 'name="tm_clear_site_password"' in template
    assert "清空已保存的站点密码" in template
    assert "留空则保持当前站点密码" in template

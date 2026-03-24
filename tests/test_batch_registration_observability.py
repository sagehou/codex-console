from pathlib import Path


def test_generic_batch_polling_uses_registration_batch_endpoint():
    script = Path("static/js/app.js").read_text(encoding="utf-8")

    assert "api.get(`/registration/batch/${batchId}`)" in script


def test_pipeline_mode_logs_wait_before_next_task():
    route_file = Path("src/web/routes/registration.py").read_text(encoding="utf-8")

    assert "[系统] 等待 {wait_time} 秒后启动下一个任务" in route_file

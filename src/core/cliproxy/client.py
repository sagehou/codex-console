"""Thin CLIProxy API client."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from curl_cffi import requests as cffi_requests


class CLIProxyAPIClient:
    def __init__(self, base_url: str, token: Optional[str] = None, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.token = token or ""
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, method: str, path: str, json: Optional[Dict[str, Any]] = None) -> Any:
        response = cffi_requests.request(
            method=method,
            url=f"{self.base_url}{path}",
            headers=self._headers(),
            json=json,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def fetch_inventory(self) -> List[Dict[str, Any]]:
        payload = self._request("GET", "/inventory")
        if isinstance(payload, dict):
            records = payload.get("items") or payload.get("data") or payload.get("records") or []
            return list(records)
        return list(payload)

    def probe_usage(self, remote_file_id: str) -> Dict[str, Any]:
        return dict(self._request("POST", f"/inventory/{remote_file_id}/probe"))

    def disable_auth(self, remote_file_id: str) -> Dict[str, Any]:
        return dict(self._request("POST", f"/inventory/{remote_file_id}/disable"))

    def reenable_auth(self, remote_file_id: str) -> Dict[str, Any]:
        return dict(self._request("POST", f"/inventory/{remote_file_id}/reenable"))

"""VGS tokenization client.

This client posts JSON payloads to a VGS inbound proxy (sandbox or live)
where aliasing rules are configured. The proxy returns the same payload
with sensitive fields replaced by VGS aliases (tokens).

Notes
- Do not hardcode secrets; pass headers via environment variables.
- Configure VGS routes/rules in the VGS dashboard to alias the target fields.
- This client is schema-agnostic: it forwards any JSON and returns the
  transformed JSON as provided by the proxy.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import json
import os

import httpx


class VGSClient:
    """HTTP client for VGS inbound proxy.

    Parameters
    - proxy_url: Base VGS proxy URL, e.g. "https://tntXXX.sandbox.verygoodproxy.com"
    - route_path: Path on the proxy configured to apply aliasing, e.g. "/post" or "/tokenize"
    - extra_headers: Optional dict of headers (e.g., auth) to send with each request
    - timeout: Request timeout in seconds
    """

    def __init__(
        self,
        proxy_url: str,
        route_path: str = "/post",
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base = proxy_url.rstrip("/")
        self.path = route_path if route_path.startswith("/") else f"/{route_path}"
        self.headers = extra_headers or {}
        self.timeout = timeout

    def _endpoint(self) -> str:
        return f"{self.base}{self.path}"

    def tokenize_json(self, payload: Any) -> Any:
        """Send a JSON payload to VGS proxy and return the transformed JSON.

        Raises httpx.HTTPError on network issues and ValueError on non-JSON responses.
        """
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(self._endpoint(), headers={"Content-Type": "application/json", **self.headers}, json=payload)
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError as e:
                raise ValueError(f"VGS response was not JSON: {resp.text[:200]}") from e

    def tokenize_records(self, records: List[Dict[str, Any]], batch_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """Tokenize a list of records.

        If your VGS rule expects the JSON array at a specific key, pass ``batch_key``
        (e.g., "records"). Otherwise the raw array will be sent as the body.
        Returns the transformed list of records.
        """
        payload = {batch_key: records} if batch_key else records
        transformed = self.tokenize_json(payload)
        if batch_key:
            if not isinstance(transformed, dict) or batch_key not in transformed:
                raise ValueError("Unexpected VGS response shape for batch_key mode")
            out = transformed.get(batch_key)
        else:
            out = transformed
        if not isinstance(out, list):
            raise ValueError("VGS response is not a list of records")
        return out


def from_env() -> VGSClient:
    """Construct a VGSClient from environment variables.

    Required env vars:
    - VGS_PROXY_URL (e.g., https://tntXXX.sandbox.verygoodproxy.com)
    Optional:
    - VGS_ROUTE_PATH (default: /post)
    - VGS_HEADERS_JSON (JSON object of headers to include)
    - VGS_TIMEOUT (seconds)
    """
    proxy = os.environ.get("VGS_PROXY_URL")
    if not proxy:
        raise RuntimeError("VGS_PROXY_URL is not set")
    route = os.environ.get("VGS_ROUTE_PATH", "/post")
    hdrs = os.environ.get("VGS_HEADERS_JSON")
    headers = json.loads(hdrs) if hdrs else None
    timeout = float(os.environ.get("VGS_TIMEOUT", "30"))
    return VGSClient(proxy, route, headers, timeout)


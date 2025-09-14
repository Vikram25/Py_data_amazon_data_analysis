"""FastAPI service to tokenize PII via Very Good Security (VGS).

Expose a simple /tokenize endpoint that accepts JSON records and returns
the transformed (tokenized) payload, delegating to the VGS inbound proxy.

Configure via env vars:
- VGS_PROXY_URL (required)
- VGS_ROUTE_PATH (default: /post)
- VGS_HEADERS_JSON (optional JSON of additional headers)
- VGS_TIMEOUT (seconds, default 30)
"""

import json
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.data_pipeline.pii.vgs_client import from_env, VGSClient


app = FastAPI(title="PII Tokenization API (VGS)")


class TokenizeRequest(BaseModel):
    records: List[Dict[str, Any]]
    batch_key: Optional[str] = None


class TokenizeResponse(BaseModel):
    records: List[Dict[str, Any]]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/tokenize", response_model=TokenizeResponse)
def tokenize(req: TokenizeRequest) -> TokenizeResponse:
    try:
        client: VGSClient = from_env()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"VGS client init failed: {e}")

    try:
        out = client.tokenize_records(req.records, batch_key=req.batch_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"VGS tokenization failed: {e}")
    return TokenizeResponse(records=out)


# Local dev: `uvicorn apps.tokenization_api:app --reload --port 8080`


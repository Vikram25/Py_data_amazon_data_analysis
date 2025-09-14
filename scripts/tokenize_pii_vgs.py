"""CLI to tokenize PII in a cleaned CSV using the Tokenization API.

Reads a cleaned CSV, posts rows in batches to the local API (or any URL)
backed by VGS, and writes a tokenized CSV as output.

Environment:
- TOKEN_API_URL (default: http://127.0.0.1:8080/tokenize)
"""

import os
import sys
import argparse
import json
from typing import List, Dict, Any

import pandas as pd
import httpx


def post_tokenize(records: List[Dict[str, Any]], api_url: str, batch_key: str | None = None, timeout: float = 30.0) -> List[Dict[str, Any]]:
    payload: Dict[str, Any] = {"records": records}
    if batch_key:
        payload["batch_key"] = batch_key
    with httpx.Client(timeout=timeout) as client:
        resp = client.post(api_url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data.get("records", [])


def main():
    ap = argparse.ArgumentParser(description="Tokenize PII via the Tokenization API (VGS-backed)")
    ap.add_argument("--input", required=True, help="Cleaned input CSV")
    ap.add_argument("--output", required=True, help="Output tokenized CSV")
    ap.add_argument("--batch-size", type=int, default=500, help="Rows per API call")
    ap.add_argument("--api-url", default=os.environ.get("TOKEN_API_URL", "http://127.0.0.1:8080/tokenize"), help="Tokenization API URL")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    out_rows: List[Dict[str, Any]] = []

    for i in range(0, len(df), args.batch_size):
        chunk = df.iloc[i:i+args.batch_size]
        recs = chunk.to_dict(orient="records")
        toks = post_tokenize(recs, api_url=args.api_url)
        if len(toks) != len(recs):
            raise RuntimeError(f"Tokenization API returned {len(toks)} records, expected {len(recs)}")
        out_rows.extend(toks)

    pd.DataFrame(out_rows).to_csv(args.output, index=False)
    print(f"Tokenized {len(out_rows)} rows -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


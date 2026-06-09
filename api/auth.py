"""
Simple API key authentication for the pipeline read API.
Set API_KEY env var to change from the dev default.
Production would use AWS API Gateway + Cognito or IAM auth.
"""
import os
import logging
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

log = logging.getLogger("auth")
API_KEY      = os.getenv("API_KEY", "dev-key-change-in-prod")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(key: str = Security(api_key_header)) -> str:
    if not key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="X-API-Key header is required")
    if key != API_KEY:
        log.warning("invalid api key attempt: %s...", key[:8])
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key")
    return key
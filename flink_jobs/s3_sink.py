"""
S3 cold-storage sink.

Buffers enriched events and writes them as Parquet to S3 every N seconds,
partitioned by date (s3://bucket/events/dt=YYYY-MM-DD/<uuid>.parquet).
This is the cold path of the Lambda architecture - ClickHouse holds 90 days
of hot data, S3 holds everything for long-term retention and batch reprocessing.

No-op if AWS credentials / bucket aren't configured, so the pipeline still
runs fully offline. Set S3_BUCKET + AWS creds in .env to enable.
"""
import io
import os
import time
import uuid
import logging
from datetime import datetime, timezone

log = logging.getLogger("s3-sink")

S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
S3_PREFIX = os.getenv("S3_PREFIX", "events").strip("/")
S3_FLUSH_S = int(os.getenv("S3_FLUSH_S", "60"))
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


class S3Sink:
    def __init__(self):
        self.enabled = bool(S3_BUCKET)
        self._buf = []
        self._last = time.time()
        self._client = None
        if not self.enabled:
            log.info("S3 sink disabled (set S3_BUCKET to enable)")
            return
        try:
            import boto3
            self._client = boto3.client("s3", region_name=AWS_REGION)
            log.info("S3 sink enabled -> s3://%s/%s/", S3_BUCKET, S3_PREFIX)
        except Exception as e:
            log.warning("S3 sink init failed (%s) - disabling", e)
            self.enabled = False

    def add(self, row):
        if self.enabled:
            self._buf.append(row)

    def maybe_flush(self):
        if not self.enabled or not self._buf:
            return
        if time.time() - self._last < S3_FLUSH_S:
            return
        self.flush()

    def flush(self):
        if not self.enabled or not self._buf:
            return
        try:
            import pandas as pd
            df = pd.DataFrame(self._buf)
            buf = io.BytesIO()
            df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
            buf.seek(0)
            dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            key = f"{S3_PREFIX}/dt={dt}/{uuid.uuid4()}.parquet"
            self._client.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
            log.info("wrote %d events -> s3://%s/%s", len(self._buf), S3_BUCKET, key)
        except Exception as e:
            log.error("s3 flush failed: %s", e)
        finally:
            self._buf.clear()
            self._last = time.time()

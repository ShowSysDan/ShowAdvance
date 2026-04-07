"""
SeaweedFS / S3-compatible object storage for 321Theater.

Credentials are read from db_config.ini [seaweedfs] section (same file used
for PostgreSQL credentials).  Results are cached for 30 s to avoid re-reading
the file on every request.

Key naming scheme (single bucket):
  attachments/{show_id}/{aid}/{filename}
  exports/{show_id}/{export_type}/v{version}.pdf
  asset-photos/{type_id}
  external-rentals/{er_id}/{filename}
"""
import configparser
import os
import time

# ─── Settings Cache ────────────────────────────────────────────────────────────
_settings_cache: dict = {}
_settings_ts: float = 0.0
_CACHE_TTL = 30  # seconds

_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'db_config.ini')


def read_s3_settings() -> dict:
    """Read SeaweedFS credentials from db_config.ini [seaweedfs].  Cached 30 s."""
    global _settings_cache, _settings_ts
    if _settings_cache and (time.time() - _settings_ts) < _CACHE_TTL:
        return _settings_cache
    result = {}
    if os.path.exists(_CONFIG_PATH):
        try:
            cp = configparser.ConfigParser()
            cp.read(_CONFIG_PATH, encoding='utf-8')
            if 'seaweedfs' in cp:
                sec = cp['seaweedfs']
                result = {
                    's3_endpoint':   sec.get('endpoint', '').rstrip('/'),
                    's3_access_key': sec.get('access_key', ''),
                    's3_secret_key': sec.get('secret_key', ''),
                    's3_bucket':     sec.get('bucket', '321theater'),
                }
        except Exception:
            pass
    _settings_cache = result
    _settings_ts = time.time()
    return result


def clear_settings_cache():
    """Invalidate the settings cache (call after saving S3 settings)."""
    global _settings_cache, _settings_ts
    _settings_cache = {}
    _settings_ts = 0.0


def is_configured() -> bool:
    """Return True if SeaweedFS credentials are present in db_config.ini."""
    cfg = read_s3_settings()
    return bool(cfg.get('s3_endpoint') and cfg.get('s3_access_key') and cfg.get('s3_secret_key'))


def get_client():
    """Return a boto3 S3 client pointed at SeaweedFS."""
    import boto3
    from botocore.config import Config
    cfg = read_s3_settings()
    return boto3.client(
        's3',
        endpoint_url=cfg['s3_endpoint'],
        aws_access_key_id=cfg['s3_access_key'],
        aws_secret_access_key=cfg['s3_secret_key'],
        config=Config(signature_version='s3v4'),
    )


def _bucket() -> str:
    return read_s3_settings().get('s3_bucket', '321theater')


def upload_file(key: str, data: bytes, content_type: str = 'application/octet-stream') -> None:
    """Upload *data* to S3 at *key*.  Raises on failure."""
    import io
    client = get_client()
    client.put_object(
        Bucket=_bucket(),
        Key=key,
        Body=io.BytesIO(data),
        ContentType=content_type,
        ContentLength=len(data),
    )


def download_file(key: str) -> bytes:
    """Download and return bytes for *key*.  Raises on failure."""
    client = get_client()
    resp = client.get_object(Bucket=_bucket(), Key=key)
    return resp['Body'].read()


def delete_file(key: str) -> None:
    """Delete *key* from S3.  Raises on failure."""
    client = get_client()
    client.delete_object(Bucket=_bucket(), Key=key)


def test_connection() -> dict:
    """
    Verify connectivity by uploading, reading back, and deleting a test object.
    Returns {"success": bool, "message": str, "endpoint": str, "bucket": str}.
    """
    cfg = read_s3_settings()
    endpoint = cfg.get('s3_endpoint', '')
    bucket = cfg.get('s3_bucket', '')
    if not is_configured():
        return {'success': False, 'message': 'SeaweedFS not configured in db_config.ini.', 'endpoint': endpoint, 'bucket': bucket}
    test_key = '_321theater_s3_test.txt'
    test_data = b'321Theater S3 connectivity test'
    try:
        upload_file(test_key, test_data, 'text/plain')
        fetched = download_file(test_key)
        delete_file(test_key)
        if fetched != test_data:
            return {'success': False, 'message': 'Data mismatch on read-back.', 'endpoint': endpoint, 'bucket': bucket}
        return {'success': True, 'message': 'SeaweedFS connection OK.', 'endpoint': endpoint, 'bucket': bucket}
    except Exception as e:
        return {'success': False, 'message': str(e), 'endpoint': endpoint, 'bucket': bucket}

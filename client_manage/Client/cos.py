import os
import threading

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

from client_manage.Config.cos import COS_CONFIG

_lock = threading.Lock()
_clients: dict[int, CosS3Client] = {}


def _make_client() -> CosS3Client:
    config = CosConfig(
        Region=COS_CONFIG['region'],
        SecretId=COS_CONFIG['secret_id'],
        SecretKey=COS_CONFIG['secret_key'],
        Token=None,
        Scheme='https',
        PoolConnections=16,
        PoolMaxSize=16,
    )
    return CosS3Client(config)


def get_cos_client() -> CosS3Client:
    """Return a per-process CosS3Client (safe across fork)."""
    pid = os.getpid()
    client = _clients.get(pid)
    if client is not None:
        return client
    with _lock:
        client = _clients.get(pid)
        if client is None:
            client = _make_client()
            _clients[pid] = client
        return client


# Backward-compatible module-level singleton for non-DataLoader callers
# (e.g. loadModelFn / saveModelFn in trainer.py that run in the main process).
cos_client = _make_client()

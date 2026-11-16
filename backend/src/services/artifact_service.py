"""Versioned artifact files for completed simulation runs.

A published result version is the SHA-256 of the canonical JSON document stored in
``SimulationRun.result``.  The same canonical bytes are written atomically to a
version-named file on the shared artifacts volume, and only the successful
conditional publish points ``SimulationRun.artifact_path`` at it.  A superseded
worker may write its own (unreferenced) version file, but it can never overwrite
the file belonging to the current complete version, so the completion status, the
API result and the result file always describe one complete version.
"""
import hashlib
import json
import os
from pathlib import Path
from ..config.settings import ARTIFACT_DIR


def canonical_bytes(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(",",":"),allow_nan=False).encode()


def result_version(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def artifact_path(run_id,version,directory=None):
    return Path(directory or ARTIFACT_DIR)/f"run-{run_id}-{version[:16]}.json"


def write_artifact(run_id,version,result,directory=None):
    """Atomically persist the published result; content hash equals ``version``."""
    if result_version(result)!=version:raise ValueError("结果版本哈希与结果内容不一致")
    path=artifact_path(run_id,version,directory)
    path.parent.mkdir(parents=True,exist_ok=True)
    payload=canonical_bytes(result)
    temporary=path.with_name(path.name+f".tmp-{os.getpid()}-{id(result)}")
    with open(temporary,"wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary,path)
    return path


def verify_artifact(path,version):
    """Return True when the on-disk file is the requested complete version."""
    file=Path(path)
    if not file.exists():return False
    return hashlib.sha256(file.read_bytes()).hexdigest()==version

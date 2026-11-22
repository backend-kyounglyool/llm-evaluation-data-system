"""raw/cleansed 저장소 추상화. 로컬 경로와 s3:// 경로를 동일 인터페이스로 다룬다."""
from __future__ import annotations

from pathlib import Path

from common.config import resolve_path


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _fs(path: str):
    import fsspec

    return fsspec.filesystem("s3") if _is_s3(path) else fsspec.filesystem("file")


def write_text(base: str, relative: str, content: str) -> str:
    base = resolve_path(base)
    if _is_s3(base):
        full = f"{base.rstrip('/')}/{relative}"
        with _fs(base).open(full, "w") as f:
            f.write(content)
        return full
    full = Path(base) / relative
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_text(content, encoding="utf-8")
    return str(full)


def list_files(base: str, pattern: str) -> list[str]:
    base = resolve_path(base)
    if _is_s3(base):
        # fsspec s3 glob은 "bucket/key"로 스킴을 떼고 돌려주므로 read_text가 S3로 인식하도록 복원
        return sorted(f"s3://{key}" for key in _fs(base).glob(f"{base.rstrip('/')}/{pattern}"))
    root = Path(base)
    return sorted(str(p) for p in root.glob(pattern)) if root.exists() else []


def read_text(path: str) -> str:
    if _is_s3(path):
        with _fs(path).open(path, "r") as f:
            return f.read()
    return Path(path).read_text(encoding="utf-8")


def parquet_glob(base: str) -> str:
    """DuckDB·pandas에서 쓸 cleansed parquet 글롭 경로."""
    base = resolve_path(base)
    return f"{base.rstrip('/')}/dt=*/*.parquet"

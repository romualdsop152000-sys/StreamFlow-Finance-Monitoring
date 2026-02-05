from datetime import datetime, timezone
from pathlib import Path

def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
from datetime import datetime, timezone
from pathlib import Path


def _utc_today_str() -> str:
    """Retourne la date du jour en UTC au format YYYY-MM-DD."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _utc_now_str() -> str:
    """Retourne le timestamp actuel en UTC au format ISO."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_dir(path: Path) -> None:
    """Crée le répertoire s'il n'existe pas."""
    path.mkdir(parents=True, exist_ok=True)
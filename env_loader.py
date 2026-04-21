"""
공통 환경변수 로더
C:\\cowork\\.claude\\.env 에서 API 키 로드 (표준 KEY=VALUE 포맷)
"""
import re
from pathlib import Path

ENV_PATH = Path(r"C:\cowork\.claude\.env")


def load_env() -> dict:
    """표준 .env 파싱 → {KEY: value} 딕셔너리"""
    if not ENV_PATH.exists():
        return {}
    result = {}
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r'^([A-Z_][A-Z0-9_]*)=(.*)$', line)
        if m:
            result[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return result


def get_key(name: str) -> str | None:
    """키 이름으로 값 조회. 환경변수 우선, 없으면 .env 파일 조회"""
    import os
    return os.environ.get(name) or load_env().get(name)

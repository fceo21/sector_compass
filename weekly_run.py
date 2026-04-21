"""
주간 자동화 스크립트
매주 월요일 12:30 KST 실행 (GitHub Actions: 03:30 UTC)

흐름:
  1. 이번 주 ISO week 계산
  2. collect  — 이번 주 신규 리포트 수집
  3. enrich   — 신규 건 섹터 정제 (Phase D+E)
  4. tone_llm — 신규 건 LLM 톤 분류
  5. render   — 이번 주 HTML 생성
  6. vercel.json 업데이트 (최신 주차로 redirect)
  7. git commit + push
"""
import sys
import json
import subprocess
from datetime import date, timedelta
from pathlib import Path

BASE = Path(__file__).parent


def this_iso_week() -> str:
    """오늘 날짜 기준 ISO week 문자열 반환. 예: '2026-W17'"""
    today = date.today()
    iso = today.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def week_monday(iso_week: str) -> date:
    """ISO week → 월요일 date"""
    year, week = int(iso_week[:4]), int(iso_week[6:])
    jan4 = date(year, 1, 4)
    week1_mon = jan4 - timedelta(days=jan4.weekday())
    return week1_mon + timedelta(weeks=week - 1)


def run(cmd: list[str], desc: str):
    print(f"\n{'='*50}")
    print(f"[STEP] {desc}")
    print(f"{'='*50}")
    result = subprocess.run(
        [sys.executable] + cmd,
        cwd=BASE,
        capture_output=False,
    )
    if result.returncode != 0:
        print(f"[ERROR] {desc} 실패 (exit {result.returncode})")
        sys.exit(result.returncode)


def update_vercel_json(iso_week: str):
    """vercel.json의 redirect destination을 최신 주차로 업데이트"""
    vpath = BASE / "vercel.json"
    config = {
        "redirects": [
            {
                "source": "/",
                "destination": f"/compass_{iso_week}.html",
                "permanent": False
            }
        ]
    }
    vpath.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[vercel.json] → compass_{iso_week}.html")


def git_push(iso_week: str):
    """변경된 파일 commit & push"""
    files = [
        f"compass_{iso_week}.html",
        "vercel.json",
        "raw_reports.db",
    ]
    subprocess.run(["git", "add"] + files, cwd=BASE)
    subprocess.run(
        ["git", "commit", "-m", f"auto: {iso_week} 섹터나침반 업데이트"],
        cwd=BASE,
    )
    result = subprocess.run(["git", "push"], cwd=BASE)
    if result.returncode == 0:
        print(f"[git push] 완료")
    else:
        print(f"[git push] 실패 — 수동 push 필요")


def main():
    iso_week = this_iso_week()
    monday   = week_monday(iso_week)
    sunday   = monday + timedelta(days=6)

    print(f"\n🧭 K-FIN 섹터나침반 주간 자동화")
    print(f"   대상 주차: {iso_week} ({monday} ~ {sunday})")

    # 1. 수집 (이번 주 월요일 ~ 오늘)
    run(
        ["collect.py", "--from", monday.isoformat(), "--to", date.today().isoformat()],
        f"수집: {monday} ~ {date.today()}"
    )

    # 2. 섹터 정제 (신규 건 대상 Phase D+E)
    run(["enrich.py", "--phase", "d"], "enrich Phase D (키워드 기타 분류)")
    run(["enrich.py", "--phase", "e"], "enrich Phase E (섹터 정규화)")

    # 3. LLM 톤 분류 (이번 주 신규 건만)
    run(
        ["tone_llm.py", "--since", monday.isoformat()],
        f"LLM 톤 분류: {monday} 이후 신규 건"
    )

    # 4. HTML 렌더
    run(["render.py", iso_week], f"렌더: {iso_week}")

    # 5. vercel.json 업데이트
    update_vercel_json(iso_week)

    # 6. git commit + push
    git_push(iso_week)

    print(f"\n✅ 완료: compass_{iso_week}.html 배포")


if __name__ == "__main__":
    main()

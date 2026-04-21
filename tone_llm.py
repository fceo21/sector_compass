"""
Groq API 기반 tone 분류기 (Llama3 무료 모델)
- 모델: llama3-8b-8192 (무료 티어, 30 RPM)
- 배치: 10건씩 묶어 1회 API 호출
- 결과: 긍정 / 부정 / 중립

실행: python tone_llm.py           (전체 재분류)
      python tone_llm.py --dry-run  (API 호출 없이 대상 건수만 확인)
"""
import re
import sys
import time
import sqlite3
from pathlib import Path
from env_loader import get_key

DB_PATH  = Path(__file__).parent / "raw_reports.db"
MODEL    = "llama-3.1-8b-instant"   # Groq 무료: 30 RPM / 14,400 req/day
BATCH    = 10     # 1회 API 요청당 리포트 수
DELAY    = 2.5    # 초 (30 RPM = 2초 간격, 여유 포함)

SYSTEM_PROMPT = """당신은 한국 주식시장 투자 리포트 분석 전문가입니다.
주어진 금융 리포트 각각의 투자 센티먼트를 분류합니다.

분류 기준:
- 긍정: 목표주가 상향, 실적 호조, 업황 개선, 비중확대, 매수 권고 등
- 부정: 목표주가 하향, 실적 부진, 업황 악화, 비중축소, 매도/중립 하향 등
- 중립: 단순 업황 설명, 방향성 불명확, 데이터 업데이트 위주

규칙:
- 반드시 번호: 결과 형식으로만 답하세요
- 다른 설명 일절 금지
- 맥락 반전에 주의 ("우려 해소"→긍정, "부진 탈출"→긍정, "성장 둔화"→부정)"""


def _build_user_prompt(batch: list[dict]) -> str:
    lines = ["다음 리포트들의 투자 센티먼트를 분류하세요.\n"]
    for i, r in enumerate(batch, 1):
        abstract_short = (r.get("abstract") or "")[:200].replace("\n", " ")
        lines.append(
            f"{i}. 섹터: {r['sector']} | 제목: {r['title']}\n"
            f"   요약: {abstract_short}\n"
        )
    lines.append("\n답변 형식 (번호: 긍정/부정/중립):")
    return "\n".join(lines)


def _parse_response(text: str, count: int) -> list[str]:
    """응답 텍스트에서 [긍정/부정/중립] 리스트 추출"""
    results = {}
    for line in text.splitlines():
        m = re.search(r'(\d+)\s*[:.\)]\s*(긍정|부정|중립)', line)
        if m:
            results[int(m.group(1))] = m.group(2)
    return [results.get(i, "중립") for i in range(1, count + 1)]


class QuotaExhausted(Exception):
    """일일 할당량 소진 — 즉시 중단 신호"""
    pass


def classify_batch(batch: list[dict], client) -> list[str] | None:
    """
    배치 분류 — Groq API 호출
    반환: tone 리스트 | None (일시 오류, 기존값 유지) | QuotaExhausted 예외
    """
    prompt = _build_user_prompt(batch)
    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.1,
            max_tokens=512,
        )
        return _parse_response(response.choices[0].message.content, len(batch))
    except Exception as e:
        msg = str(e)
        # 일일 할당량 소진 → 즉시 중단
        if "429" in msg and ("day" in msg.lower() or "quota" in msg.lower()):
            raise QuotaExhausted(f"일일 할당량 소진: {MODEL}")
        # RPM 초과 → 잠깐 대기 후 재시도 1회
        if "429" in msg:
            print(f"  [RPM 초과, 10초 대기 후 재시도]")
            time.sleep(10)
            try:
                response = client.chat.completions.create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": prompt},
                    ],
                    temperature=0.1,
                    max_tokens=512,
                )
                return _parse_response(response.choices[0].message.content, len(batch))
            except Exception:
                pass
        # 그 외 오류 → 기존 tone 유지
        print(f"  [API 오류, 기존값 유지] {msg[:120]}")
        return None


def run(dry_run: bool = False, since: str | None = None):
    """전체 실행. since=YYYY-MM-DD 이면 해당 날짜 이후 신규 건만 처리"""
    api_key = get_key("GROQ_API_KEY")
    if not api_key:
        print("오류: GROQ_API_KEY를 찾을 수 없습니다 (C:\\cowork\\.claude\\.env)")
        return

    conn = sqlite3.connect(DB_PATH)
    if since:
        # 신규 수집분 중 tone이 기본값(키워드 기반)인 것만 재분류
        rows = conn.execute("""
            SELECT id,
                   COALESCE(sector_refined, sector) AS sector,
                   title,
                   abstract
            FROM raw_reports
            WHERE abstract IS NOT NULL AND abstract != ''
              AND report_date >= ?
            ORDER BY report_date DESC
        """, (since,)).fetchall()
        print(f"[since {since}] 신규 대상: {len(rows)}건")
    else:
        # 전체 재분류
        rows = conn.execute("""
            SELECT id,
                   COALESCE(sector_refined, sector) AS sector,
                   title,
                   abstract
            FROM raw_reports
            WHERE abstract IS NOT NULL AND abstract != ''
            ORDER BY report_date DESC
        """).fetchall()
    conn.close()

    total = len(rows)
    print(f"분류 대상: {total}건 / 배치 {BATCH}건 / 예상 소요: ~{total//BATCH * DELAY / 60:.0f}분")

    if dry_run:
        print("--dry-run 모드: API 호출 없이 종료")
        return

    # Groq 클라이언트 초기화
    from groq import Groq
    client = Groq(api_key=api_key)

    updated = changed = 0
    dist = {"긍정": 0, "부정": 0, "중립": 0}

    for i in range(0, total, BATCH):
        batch_rows = rows[i: i + BATCH]
        batch = [{"id": r[0], "sector": r[1], "title": r[2], "abstract": r[3]}
                 for r in batch_rows]

        try:
            tones = classify_batch(batch, client)
        except QuotaExhausted as e:
            print(f"\n\n[중단] {e}")
            print(f"  진행: {updated}/{total}건 처리, {changed}건 변경")
            print(f"  내일 다시 실행하면 이어서 처리됩니다.")
            break

        # None = API 오류 → 이 배치 건너뜀 (기존 tone 유지)
        if tones is None:
            updated += len(batch)
            time.sleep(DELAY)
            continue

        conn = sqlite3.connect(DB_PATH)
        for item, tone in zip(batch, tones):
            old = conn.execute("SELECT tone FROM raw_reports WHERE id=?",
                               (item["id"],)).fetchone()[0]
            conn.execute("UPDATE raw_reports SET tone=? WHERE id=?",
                         (tone, item["id"]))
            dist[tone] = dist.get(tone, 0) + 1
            if old != tone:
                changed += 1
            updated += 1
        conn.commit()
        conn.close()

        done = min(i + BATCH, total)
        pct  = done / total * 100
        print(f"  [{pct:5.1f}%] {done}/{total}  변경: {changed}건", end="\r")
        time.sleep(DELAY)

    print(f"\n완료: {updated}건 처리 / {changed}건 변경")
    print("tone 분포:", dist)


if __name__ == "__main__":
    dry   = "--dry-run" in sys.argv
    since = None
    if "--since" in sys.argv:
        since = sys.argv[sys.argv.index("--since") + 1]
    run(dry_run=dry, since=since)

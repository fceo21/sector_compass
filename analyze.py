"""
STEP 2: 섹터나침반 분석 엔진
raw_reports.db → 주간 집계 + 4가지 지표 계산
(전략과 무관한 순수 측정 레이어)
"""
import sqlite3
import json
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict

DB_PATH = Path(__file__).parent / "raw_reports.db"


def iso_week(d: date) -> str:
    """날짜 → 'YYYY-WXX' ISO 주차 문자열"""
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def week_monday(iso_week_str: str) -> date:
    """'YYYY-WXX' → 해당 주 월요일"""
    y, w = int(iso_week_str[:4]), int(iso_week_str[6:])
    return date.fromisocalendar(y, w, 1)


# ──────────────────────────────────────────────
# 1. 주간 집계 (raw → weekly_sector_stats)
# ──────────────────────────────────────────────

def build_weekly_stats() -> dict:
    """
    반환: {
      'YYYY-WXX': {
        '섹터명': {
          'count':      int,          # 총 리포트 수
          'firms':      set(str),     # 참여 증권사 집합
          'tone_pos':   int,          # 긍정 건수
          'tone_neg':   int,          # 부정 건수
          'tone_neu':   int,          # 중립 건수
          'deep':       int,          # 심층 리포트 수
        }
      }
    }
    """
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """SELECT report_date,
                  COALESCE(sector_refined, sector) AS sector,
                  firm, tone, report_type
           FROM raw_reports ORDER BY report_date"""
    ).fetchall()
    conn.close()

    stats = defaultdict(lambda: defaultdict(lambda: {
        'count': 0, 'firms': set(),
        'tone_pos': 0, 'tone_neg': 0, 'tone_neu': 0, 'deep': 0
    }))

    for report_date, sector, firm, tone, rtype in rows:
        d = date.fromisoformat(report_date)
        wk = iso_week(d)
        s = stats[wk][sector]
        s['count'] += 1
        s['firms'].add(firm)
        if tone == '긍정':   s['tone_pos'] += 1
        elif tone == '부정': s['tone_neg'] += 1
        else:                s['tone_neu'] += 1
        if rtype == '심층':  s['deep'] += 1

    return stats


# ──────────────────────────────────────────────
# 2. 4가지 지표 계산
# ──────────────────────────────────────────────

def compute_indicators(weekly_stats: dict, target_week: str, lookback: int = 4) -> list[dict]:
    """
    target_week 기준으로 각 섹터의 4가지 지표 계산
    반환: [{ sector, count, delta, tone_ratio, firm_count, dormancy_weeks }, ...]
    """
    all_weeks = sorted(weekly_stats.keys())
    if target_week not in all_weeks:
        return []

    tw_idx = all_weeks.index(target_week)

    # 직전 lookback 주 리스트
    prev_weeks = all_weeks[max(0, tw_idx - lookback): tw_idx]

    # 전체 섹터 목록 (target 포함 모든 주)
    all_sectors = set()
    for wk_data in weekly_stats.values():
        all_sectors.update(wk_data.keys())

    results = []
    cur_data = weekly_stats.get(target_week, {})

    for sector in sorted(all_sectors):
        s = cur_data.get(sector, {
            'count': 0, 'firms': set(),
            'tone_pos': 0, 'tone_neg': 0, 'tone_neu': 0, 'deep': 0
        })

        count      = s['count']
        firm_count = len(s['firms'])

        # 1) Coverage Delta: 이번주 건수 / 직전 4주 평균 - 1
        prev_counts = [weekly_stats.get(w, {}).get(sector, {}).get('count', 0)
                       for w in prev_weeks]
        avg_prev = sum(prev_counts) / len(prev_counts) if prev_counts else 0
        if avg_prev > 0:
            delta = (count - avg_prev) / avg_prev   # -1 ~ ∞
        elif count > 0:
            delta = 1.0   # 이전에 없다가 등장 = +100%
        else:
            delta = 0.0

        # 2) Tone Ratio: (긍정 - 부정) / 전체  (-1 ~ 1)
        total = count if count > 0 else 1
        tone_ratio = (s['tone_pos'] - s['tone_neg']) / total

        # 3) Dormancy: 직전 몇 주 연속 0건이었나
        dormancy = 0
        for w in reversed(prev_weeks):
            c = weekly_stats.get(w, {}).get(sector, {}).get('count', 0)
            if c == 0:
                dormancy += 1
            else:
                break

        results.append({
            'sector':         sector,
            'count':          count,
            'avg_prev':       round(avg_prev, 1),
            'delta':          round(delta, 2),
            'tone_pos':       s['tone_pos'],
            'tone_neg':       s['tone_neg'],
            'tone_neu':       s['tone_neu'],
            'tone_ratio':     round(tone_ratio, 2),
            'firm_count':     firm_count,
            'firms':          sorted(s['firms']),
            'deep_ratio':     round(s['deep'] / total, 2),
            'dormancy_weeks': dormancy,
        })

    # 건수 있는 섹터 + 신호 있는 섹터만 (완전 무관심 제거)
    results = [r for r in results
               if r['count'] > 0 or r['dormancy_weeks'] > 0]

    # count 내림차순 정렬
    results.sort(key=lambda x: x['count'], reverse=True)
    return results


# ──────────────────────────────────────────────
# 3. 전체 주차 목록 조회
# ──────────────────────────────────────────────

def get_all_weeks() -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT DISTINCT report_date FROM raw_reports ORDER BY report_date").fetchall()
    conn.close()
    weeks = sorted(set(iso_week(date.fromisoformat(r[0])) for r in rows))
    return weeks


# ──────────────────────────────────────────────
# 4. 데이터 요약 출력 (빠른 확인용)
# ──────────────────────────────────────────────

def summary():
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM raw_reports").fetchone()[0]
    date_range = conn.execute(
        "SELECT MIN(report_date), MAX(report_date) FROM raw_reports"
    ).fetchone()
    sectors = conn.execute(
        "SELECT sector, COUNT(*) c FROM raw_reports GROUP BY sector ORDER BY c DESC"
    ).fetchall()
    firms = conn.execute(
        "SELECT firm, COUNT(*) c FROM raw_reports GROUP BY firm ORDER BY c DESC LIMIT 10"
    ).fetchall()
    tones = conn.execute(
        "SELECT tone, COUNT(*) c FROM raw_reports GROUP BY tone"
    ).fetchall()
    conn.close()

    print(f"총 {total}건  |  {date_range[0]} ~ {date_range[1]}")
    print()
    print("[ 섹터별 건수 ]")
    for s, c in sectors:
        bar = '#' * (c // 10)
        print(f"  {s:12s} {c:4d} {bar}")
    print()
    print("[ 증권사 TOP10 ]")
    for f, c in firms:
        print(f"  {f:12s} {c:4d}")
    print()
    print("[ 톤 분포 ]")
    for t, c in tones:
        print(f"  {t}: {c}")


if __name__ == "__main__":
    print("=== DB 요약 ===")
    summary()

    print("\n=== 주차 목록 ===")
    weeks = get_all_weeks()
    print(f"총 {len(weeks)}주: {weeks[0]} ~ {weeks[-1]}")

    print("\n=== 최근 주 지표 샘플 (마지막 주) ===")
    stats = build_weekly_stats()
    latest = weeks[-1]
    indicators = compute_indicators(stats, latest)
    for r in indicators[:8]:
        print(
            f"  {r['sector']:12s} "
            f"건수:{r['count']:3d} "
            f"delta:{r['delta']:+.0%} "
            f"톤:{r['tone_ratio']:+.2f} "
            f"증권사:{r['firm_count']} "
            f"휴면:{r['dormancy_weeks']}주"
        )

"""
STEP 1: Naver Finance 산업리포트 대량 수집
page 1~49 → raw_reports.db 저장
"""
import sqlite3
import time
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from pathlib import Path

DB_PATH = Path(__file__).parent / "raw_reports.db"
BASE_URL = "https://finance.naver.com/research/industry_list.naver"
DETAIL_URL = "https://finance.naver.com/research/industry_read.naver"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# 수집 범위
DATE_FROM = date(2026, 1, 1)
DATE_TO   = date(2026, 4, 20)


def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_reports (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,        -- YYYY-MM-DD
            sector      TEXT NOT NULL,
            title       TEXT NOT NULL,
            firm        TEXT NOT NULL,
            pdf_url     TEXT,
            tone        TEXT,                 -- 긍정/부정/중립
            report_type TEXT,                 -- 심층/기타
            nid         INTEGER,              -- Naver 리포트 ID
            abstract    TEXT,                 -- 리포트 요약 텍스트
            UNIQUE(report_date, firm, title)
        )
    """)
    # 기존 DB에 컬럼 없으면 추가
    try:
        conn.execute("ALTER TABLE raw_reports ADD COLUMN nid INTEGER")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE raw_reports ADD COLUMN abstract TEXT")
    except Exception:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date   ON raw_reports(report_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sector ON raw_reports(sector)")
    conn.commit()


EXCLUDE_KEYWORDS = re.compile(
    r'weekly|위클리|주간|월간|속보|모닝|daily|프리뷰|morning|데일리',
    re.IGNORECASE
)

POSITIVE_KW = re.compile(
    r'상향|긍정|호조|급증|수혜|기대|성장|강세|회복|개선|확대|상승|증가|호실적|상회|'
    r'역대|최대|최고|반등|턴어라운드|모멘텀|기회|선호|비중확대|매수'
)
NEGATIVE_KW = re.compile(
    r'하향|부정|부진|감소|우려|리스크|약세|침체|악화|축소|하락|둔화|적자|손실|'
    r'하회|최저|저조|하강|비중축소|매도|경고|위험|불확실'
)


def infer_tone(title: str) -> str:
    pos = len(POSITIVE_KW.findall(title))
    neg = len(NEGATIVE_KW.findall(title))
    if pos > neg:   return "긍정"
    if neg > pos:   return "부정"
    return "중립"


def infer_type(title: str) -> str:
    if EXCLUDE_KEYWORDS.search(title):
        return "기타"
    return "심층"


def parse_page(page: int) -> list[dict]:
    r = requests.get(BASE_URL, params={"page": page}, headers=HEADERS, timeout=15)
    r.encoding = "EUC-KR"
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="type_1")
    if not table:
        return []

    reports = []
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        sector = cols[0].get_text(strip=True)
        title  = cols[1].get_text(strip=True)
        firm   = cols[2].get_text(strip=True)
        date_s = cols[4].get_text(strip=True)  # YY.MM.DD

        if not sector or not title or not date_s:
            continue
        if not re.match(r'\d{2}\.\d{2}\.\d{2}', date_s):
            continue

        # nid 추출 (cols[1] 링크: industry_read.naver?nid=XXXXX)
        a_tag = cols[1].find("a", href=True)
        nid = None
        if a_tag:
            m = re.search(r'nid=(\d+)', a_tag.get("href", ""))
            nid = int(m.group(1)) if m else None

        # PDF URL 추출 (cols[3] 직접 다운로드 링크)
        pdf_url = ""
        if len(cols) > 3:
            pdf_a = cols[3].find("a", href=True)
            if pdf_a:
                pdf_url = pdf_a.get("href", "")

        try:
            report_date = datetime.strptime(date_s, "%y.%m.%d").date()
        except ValueError:
            continue

        if report_date < DATE_FROM:
            return reports  # 이 페이지 이후는 범위 밖 → 중단 신호

        if report_date > DATE_TO:
            continue  # 미래 데이터 스킵

        reports.append({
            "report_date": report_date.isoformat(),
            "sector":      sector,
            "title":       title,
            "firm":        firm,
            "pdf_url":     pdf_url,
            "nid":         nid,
            "tone":        infer_tone(title),   # 초기값; enrich.py Phase C로 개선
            "report_type": infer_type(title),
        })

    return reports


def collect(max_pages: int = 55):
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    total_inserted = 0
    total_skipped  = 0

    for page in range(1, max_pages + 1):
        try:
            rows = parse_page(page)
        except Exception as e:
            print(f"[page {page:3d}] ERROR: {e}")
            time.sleep(2)
            continue

        if not rows:
            print(f"[page {page:3d}] 데이터 없음 → 중단")
            break

        # DATE_FROM 이전 데이터가 포함된 경우 → 이 페이지가 마지막
        hit_boundary = any(r["report_date"] < DATE_FROM.isoformat() for r in rows)

        inserted = skipped = 0
        for r in rows:
            if r["report_date"] < DATE_FROM.isoformat():
                continue
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO raw_reports "
                    "(report_date, sector, title, firm, pdf_url, tone, report_type, nid) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (r["report_date"], r["sector"], r["title"],
                     r["firm"], r["pdf_url"], r["tone"], r["report_type"], r["nid"])
                )
                if conn.total_changes > total_inserted + total_skipped:
                    inserted += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1

        conn.commit()
        total_inserted += inserted
        total_skipped  += skipped

        # 이 페이지 날짜 범위 표시
        dates = [r["report_date"] for r in rows if r["report_date"] >= DATE_FROM.isoformat()]
        rng = f"{min(dates)} ~ {max(dates)}" if dates else "범위 외"
        print(f"[page {page:3d}] {rng}  +{inserted} rows  (skip {skipped})")

        if hit_boundary:
            print(f"[page {page:3d}] 1월 1일 이전 도달 → 수집 완료")
            break

        time.sleep(0.3)  # 네이버 부하 방지

    conn.close()
    print(f"\n완료: 총 {total_inserted}건 저장, {total_skipped}건 중복/스킵")
    return total_inserted


if __name__ == "__main__":
    collect()

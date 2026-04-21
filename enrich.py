"""
STEP 1.5: raw_reports.db 보강
- Phase A: 목록 페이지 재스크랩 → nid, pdf_url 업데이트
- Phase B: detail 페이지(industry_read.naver) → abstract 수집
- Phase C: abstract 기반 tone 재계산
- Phase D: '기타' 섹터 키워드 재분류 → 중간 섹터명 부여
- Phase E: 전체 sector_refined → 18개 세부섹터 체계로 정규화

실행: python enrich.py          (전체)
      python enrich.py --phase a  (Phase A만)
      python enrich.py --phase b  (Phase B만)
      python enrich.py --phase c  (Phase C만)
      python enrich.py --phase d  (Phase D만)
      python enrich.py --phase e  (Phase E만)
      python enrich.py --phase de (Phase D+E)
"""
import sqlite3
import time
import re
import sys
import requests
from bs4 import BeautifulSoup
from datetime import date, datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "raw_reports.db"
BASE_URL = "https://finance.naver.com/research/industry_list.naver"
DETAIL_URL = "https://finance.naver.com/research/industry_read.naver"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

DATE_FROM = date(2026, 1, 1)
DATE_TO   = date(2026, 4, 20)

# ──────────────────────────────────────────────────────────
# Tone 키워드 (abstract 텍스트 기반으로 더 정확)
# ──────────────────────────────────────────────────────────
POSITIVE_KW = re.compile(
    r'상향|긍정|호조|급증|수혜|기대|성장|강세|회복|개선|확대|상승|증가|호실적|상회|'
    r'역대|최대|최고|반등|턴어라운드|모멘텀|기회|선호|비중확대|매수'
)
NEGATIVE_KW = re.compile(
    r'하향|부정|부진|감소|우려|리스크|약세|침체|악화|축소|하락|둔화|적자|손실|'
    r'하회|최저|저조|하강|둔화|비중축소|매도|경고|위험|불확실'
)


def infer_tone(text: str) -> str:
    """긍정/부정 키워드 카운트 → tone 분류"""
    pos = len(POSITIVE_KW.findall(text))
    neg = len(NEGATIVE_KW.findall(text))
    if pos > neg:   return "긍정"
    if neg > pos:   return "부정"
    return "중립"


# ──────────────────────────────────────────────────────────
# Phase A: 목록 페이지 재스크랩 → nid, pdf_url 수집
# ──────────────────────────────────────────────────────────

def phase_a(max_pages: int = 55):
    """목록 페이지에서 nid + pdf_url을 추출해 DB 업데이트"""
    conn = sqlite3.connect(DB_PATH)
    updated = 0
    not_found = 0

    print("[Phase A] 목록 페이지 재스크랩 시작 (nid + pdf_url 수집)")

    for page in range(1, max_pages + 1):
        try:
            r = requests.get(BASE_URL, params={"page": page}, headers=HEADERS, timeout=15)
            r.encoding = "EUC-KR"
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table", class_="type_1")
            if not table:
                print(f"  [page {page:3d}] table 없음 → 중단")
                break
        except Exception as e:
            print(f"  [page {page:3d}] 오류: {e}")
            time.sleep(2)
            continue

        hit_boundary = False
        page_updated = 0
        rows = table.find_all("tr")

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            date_s = cols[4].get_text(strip=True)
            if not re.match(r'\d{2}\.\d{2}\.\d{2}', date_s):
                continue

            try:
                report_date = datetime.strptime(date_s, "%y.%m.%d").date()
            except ValueError:
                continue

            if report_date < DATE_FROM:
                hit_boundary = True
                continue
            if report_date > DATE_TO:
                continue

            sector = cols[0].get_text(strip=True)
            title  = cols[1].get_text(strip=True)
            firm   = cols[2].get_text(strip=True)

            # nid 추출 (cols[1] 링크)
            a_tag = cols[1].find("a", href=True)
            nid = None
            if a_tag:
                m = re.search(r'nid=(\d+)', a_tag.get("href", ""))
                nid = int(m.group(1)) if m else None

            # pdf_url 추출 (cols[3] 직접 링크)
            pdf_url = ""
            file_cell = cols[3] if len(cols) > 3 else None
            if file_cell:
                pdf_a = file_cell.find("a", href=True)
                if pdf_a:
                    pdf_url = pdf_a.get("href", "")

            if not nid:
                continue

            # DB에서 matching record 찾기 (report_date + firm + title 앞부분)
            title_clean = title.rstrip(".").rstrip(".")
            row_data = conn.execute(
                "SELECT id, title FROM raw_reports WHERE report_date=? AND firm=?",
                (report_date.isoformat(), firm)
            ).fetchall()

            matched_id = None
            for rid, db_title in row_data:
                db_clean = db_title.rstrip(".").rstrip(".")
                # 앞 15자 비교 (목록 페이지 제목 말줄임 대응)
                if (title_clean[:15] == db_clean[:15] or
                        db_clean.startswith(title_clean[:12]) or
                        title_clean.startswith(db_clean[:12])):
                    matched_id = rid
                    break

            if matched_id:
                conn.execute(
                    "UPDATE raw_reports SET nid=?, pdf_url=? WHERE id=?",
                    (nid, pdf_url, matched_id)
                )
                updated += 1
                page_updated += 1
            else:
                not_found += 1

        conn.commit()
        print(f"  [page {page:3d}] 업데이트 {page_updated}건")

        if hit_boundary:
            print(f"  [page {page:3d}] DATE_FROM 이전 도달 → 완료")
            break

        time.sleep(0.3)

    conn.close()
    print(f"\n[Phase A] 완료: nid 업데이트 {updated}건, 미매칭 {not_found}건")
    return updated


# ──────────────────────────────────────────────────────────
# Phase B: detail 페이지 → abstract 수집
# ──────────────────────────────────────────────────────────

def fetch_abstract(nid: int) -> str | None:
    """industry_read.naver 페이지에서 요약 텍스트 추출"""
    try:
        r = requests.get(DETAIL_URL, params={"nid": nid}, headers=HEADERS, timeout=15)
        r.encoding = "EUC-KR"
        soup = BeautifulSoup(r.text, "html.parser")
        td = soup.find("td", class_="view_cnt")
        if td:
            # PDF 파일명(마지막 .pdf 링크)과 "길이: N자" 제거
            text = td.get_text(strip=True)
            # ".pdf" 이후 내용 제거
            text = re.sub(r'\S+\.pdf\S*', '', text)
            # 조회수, 뷰카운트 숫자 텍스트 제거
            text = re.sub(r'조회\s*:\s*\d+', '', text)
            text = text.strip()
            return text if len(text) > 20 else None
    except Exception as e:
        print(f"    [nid={nid}] 오류: {e}")
    return None


def phase_b(batch_size: int = 50, delay: float = 0.4):
    """nid 있고 abstract 없는 레코드에 대해 detail 페이지 fetch"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, nid FROM raw_reports WHERE nid IS NOT NULL AND abstract IS NULL ORDER BY report_date DESC"
    ).fetchall()
    conn.close()

    total = len(rows)
    print(f"[Phase B] abstract 수집 대상: {total}건")
    if not total:
        print("  → 모두 완료됨")
        return

    fetched = 0
    failed  = 0

    for i, (rid, nid) in enumerate(rows, 1):
        abstract = fetch_abstract(nid)
        conn = sqlite3.connect(DB_PATH)
        if abstract:
            conn.execute("UPDATE raw_reports SET abstract=? WHERE id=?", (abstract, rid))
            fetched += 1
        else:
            # None으로 표시해서 재시도 방지 (빈 문자열)
            conn.execute("UPDATE raw_reports SET abstract='' WHERE id=?", (rid,))
            failed += 1
        conn.commit()
        conn.close()

        if i % 50 == 0:
            print(f"  진행: {i}/{total}  성공={fetched}  실패={failed}")

        time.sleep(delay)

    print(f"\n[Phase B] 완료: 성공 {fetched}건, 실패/빈 {failed}건")
    return fetched


# ──────────────────────────────────────────────────────────
# Phase C: abstract 기반 tone 재계산
# ──────────────────────────────────────────────────────────

def phase_c():
    """abstract 있는 레코드의 tone을 abstract 텍스트로 재계산"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, title, abstract, tone FROM raw_reports WHERE abstract IS NOT NULL AND abstract != ''"
    ).fetchall()

    updated = 0
    tone_dist = {"긍정": 0, "부정": 0, "중립": 0}

    for rid, title, abstract, old_tone in rows:
        new_tone = infer_tone(abstract + " " + title)
        tone_dist[new_tone] += 1
        if new_tone != old_tone:
            conn.execute("UPDATE raw_reports SET tone=? WHERE id=?", (new_tone, rid))
            updated += 1

    conn.commit()

    # 전체 tone 분포 출력
    total_abs = len(rows)
    total_all = conn.execute("SELECT COUNT(*) FROM raw_reports").fetchone()[0]
    conn.close()

    print(f"\n[Phase C] tone 재계산 완료")
    print(f"  abstract 있는 레코드: {total_abs} / 전체 {total_all}")
    print(f"  tone 변경: {updated}건")
    print(f"  새 분포 (abstract 기반):")
    for t, c in tone_dist.items():
        pct = c / total_abs * 100 if total_abs else 0
        print(f"    {t}: {c}건 ({pct:.1f}%)")
    return updated


# ──────────────────────────────────────────────────────────
# Phase D: "기타" 섹터 재분류
# ──────────────────────────────────────────────────────────

# 분류 규칙 (우선순위 순서, 앞쪽이 먼저 매칭)
SECTOR_RECLASSIFY_RULES = [
    # 신규 섹터 (네이버 미분류)
    ('로봇',      re.compile(r'로봇|휴머노이드|로보틱스')),
    ('방산',      re.compile(r'방산|방위산업|방위비|무기수출|군비|이란 사태|호르무즈.*전쟁|중동.*무기|글로벌스타|Arsenal')),
    ('2차전지',   re.compile(r'2차전지|배터리|양극재|음극재|ESS[^G]|전해질|셀메이커|배터리셀')),
    ('디지털자산', re.compile(r'디지털자산|블록체인|스테이블코인|\bRWA\b|암호화폐|가상자산|비트코인|이더리움|\bSTO\b|토큰증권')),
    ('우주',      re.compile(r'우주|위성|발사체|로켓|항공우주|SpaceX|블루오리진')),
    # 기존 섹터로 재배치
    ('에너지',    re.compile(r'\bLNG\b|호르무즈|에너지 안보|에너지공급|연료전지|카타르.*LNG|LNG.*공급')),
    ('재생에너지', re.compile(r'태양광|재생에너지|풍력|에너지전환|ESG.*[Tt]ransit|\bEUA\b|탄소배출권|K-ETS|탄소시장')),
    ('미디어',    re.compile(r'엔터테인먼트|K-pop|아이돌|드라마|하이브|BTS|HYBE|SM|JYP|뉴진스|K-컬쳐|K-뷰티.*엔터|콘텐츠 캘린더')),
    ('AI',        re.compile(r'\bAI\b|인공지능|\bLLM\b|\bGPT\b|빅테크|광통신|\bOFC\b')),
    ('전기전자',  re.compile(r'변압기|전력기기|전력인프라|초고압|전력반도체|SiC|GaN')),
    ('레저',      re.compile(r'레저|카지노|관광|리조트|인바운드|홀드율|강원랜드|GKL|하나투어')),
    ('유통',      re.compile(r'유통|리테일|트레이드다운|이커머스|소비재')),
    ('건설',      re.compile(r'주택시장|아파트|재건축|착공.*주택|주택.*착공')),
    ('화장품',    re.compile(r'스킨부스터|필러|보톡스|\bECM\b|K-뷰티')),
    ('ESG',       re.compile(r'\bESG\b|자사주.*소각|지배구조.*주총|상법개정|탄소중립')),
]


def phase_d():
    """기타 섹터 reports를 title+abstract 키워드로 재분류"""
    conn = sqlite3.connect(DB_PATH)

    # sector_refined 컬럼 추가 (없으면)
    try:
        conn.execute("ALTER TABLE raw_reports ADD COLUMN sector_refined TEXT")
        conn.commit()
        print("[Phase D] sector_refined 컬럼 추가됨")
    except Exception:
        pass

    # 1) 전체 레코드에 sector_refined = sector (초기화)
    conn.execute("UPDATE raw_reports SET sector_refined = sector WHERE sector_refined IS NULL")
    conn.commit()

    # 2) 기타 섹터만 재분류
    rows = conn.execute(
        "SELECT id, title, abstract FROM raw_reports WHERE sector = '기타'"
    ).fetchall()

    reclassified = 0
    dist = {}

    for rid, title, abstract in rows:
        text = (title or '') + ' ' + (abstract or '')[:300]
        new_sector = None
        for sector_name, pattern in SECTOR_RECLASSIFY_RULES:
            if pattern.search(text):
                new_sector = sector_name
                break

        target = new_sector if new_sector else '기타'
        dist[target] = dist.get(target, 0) + 1

        conn.execute(
            "UPDATE raw_reports SET sector_refined = ? WHERE id = ?",
            (target, rid)
        )
        if new_sector:
            reclassified += 1

    conn.commit()
    conn.close()

    print(f"\n[Phase D] 기타 섹터 재분류 완료")
    print(f"  총 {len(rows)}건 중 {reclassified}건 재분류, {len(rows)-reclassified}건 기타 유지")
    print(f"  재분류 결과:")
    for s, c in sorted(dist.items(), key=lambda x: -x[1]):
        print(f"    {s:12s}: {c}건")
    return reclassified


# ──────────────────────────────────────────────────────────
# Phase E: 전체 sector_refined → 18개 세부섹터 정규화
# ──────────────────────────────────────────────────────────

# 중간 sector_refined 값 → 최종 18개 세부섹터 매핑
SECTOR_FINAL_MAP = {
    # 반도체
    '반도체':     '반도체',

    # IT 하드웨어
    'IT':         'IT 하드웨어',
    '디스플레이': 'IT 하드웨어',
    '핸드폰':     'IT 하드웨어',
    '휴대폰':     'IT 하드웨어',
    '전기전자':   'IT 하드웨어',   # 변압기·전력기기 등 IT 장비

    # S/W 및 플랫폼
    '인터넷':     'S/W 및 플랫폼',
    '인터넷엔터': 'S/W 및 플랫폼',
    '게임':       'S/W 및 플랫폼',
    '인터넷포털': 'S/W 및 플랫폼',
    '인터넷포탈': 'S/W 및 플랫폼',   # 네이버 원본 표기

    # 신성장 기술
    '로봇':       '신성장 기술',
    'AI':         '신성장 기술',
    '우주':       '신성장 기술',
    '우주항공':   '신성장 기술',

    # 이차전지
    '2차전지':    '이차전지',

    # 전력 및 에너지
    '유틸리티':   '전력 및 에너지',
    'ESG':        '전력 및 에너지',
    '에너지':     '전력 및 에너지',
    '재생에너지': '전력 및 에너지',

    # 화학 및 소재
    '석유화학':   '화학 및 소재',
    '철강금속':   '화학 및 소재',

    # 자동차·조선
    '자동차':     '자동차·조선',
    '자동차부품': '자동차·조선',
    '조선':       '자동차·조선',
    '타이어':     '자동차·조선',
    '항공운송':   '자동차·조선',

    # 기계 및 방산
    '방산':       '기계 및 방산',
    '기계':       '기계 및 방산',

    # 건설 및 물류
    '건설':       '건설 및 물류',
    '운송':       '건설 및 물류',
    '해운':       '건설 및 물류',

    # 제약 및 바이오
    '제약':       '제약 및 바이오',
    '바이오':     '제약 및 바이오',

    # 의료기기
    '헬스케어':   '의료기기',
    '의료기기':   '의료기기',

    # 금융 및 지주
    '은행':       '금융 및 지주',
    '보험':       '금융 및 지주',
    '지주회사':   '금융 및 지주',
    '증권':       '금융 및 지주',
    '금융':       '금융 및 지주',

    # 소비재
    '화장품':     '소비재',
    '음식료':     '소비재',
    '섬유의류':   '소비재',

    # 유통 (리테일)
    '유통':       '유통',
    '인터넷쇼핑': '유통',

    # 통신 (내수 방어주)
    '이동통신':   '통신',
    '통신':       '통신',

    # 레저 및 엔터
    '레저':       '레저 및 엔터',
    '미디어':     '레저 및 엔터',
    '여행':       '레저 및 엔터',

    # 시장 센티먼트
    '디지털자산': '시장 센티먼트',

    # 미분류
    '기타':       '기타',
}


def phase_e():
    """전체 sector_refined를 18개 세부섹터 체계로 정규화"""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT id, COALESCE(sector_refined, sector) AS sr FROM raw_reports"
    ).fetchall()

    updated = 0
    unmapped = {}

    for rid, sr in rows:
        final = SECTOR_FINAL_MAP.get(sr)
        if final:
            conn.execute("UPDATE raw_reports SET sector_refined=? WHERE id=?", (final, rid))
            updated += 1
        else:
            unmapped[sr] = unmapped.get(sr, 0) + 1

    conn.commit()

    # 결과 분포
    dist = conn.execute(
        "SELECT sector_refined, COUNT(*) c FROM raw_reports GROUP BY sector_refined ORDER BY c DESC"
    ).fetchall()
    conn.close()

    print(f"\n[Phase E] 섹터 정규화 완료: {updated}건 업데이트")
    if unmapped:
        print(f"  미매핑 섹터 (기타로 유지):")
        for s, c in sorted(unmapped.items(), key=lambda x: -x[1]):
            print(f"    {s:20s} {c}건")
    print(f"\n  최종 섹터 분포:")
    for s, c in dist:
        print(f"    {s:20s} {c}건")
    return updated


# ──────────────────────────────────────────────────────────
# 진행 현황 요약
# ──────────────────────────────────────────────────────────

def status():
    conn = sqlite3.connect(DB_PATH)
    total    = conn.execute("SELECT COUNT(*) FROM raw_reports").fetchone()[0]
    has_nid  = conn.execute("SELECT COUNT(*) FROM raw_reports WHERE nid IS NOT NULL").fetchone()[0]
    has_abs  = conn.execute("SELECT COUNT(*) FROM raw_reports WHERE abstract IS NOT NULL AND abstract != ''").fetchone()[0]
    tones    = conn.execute("SELECT tone, COUNT(*) FROM raw_reports GROUP BY tone").fetchall()
    conn.close()
    print(f"전체: {total}건 | nid 있음: {has_nid} | abstract 있음: {has_abs}")
    print("tone 분포:", {t: c for t, c in tones})


# ──────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    phase = sys.argv[2] if len(sys.argv) > 2 else "all"
    if "--phase" in sys.argv:
        idx = sys.argv.index("--phase")
        phase = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else "all"

    print("=== 현재 DB 상태 ===")
    status()
    print()

    if phase in ("a", "all"):
        phase_a()
        print()
        status()
        print()

    if phase in ("b", "all"):
        phase_b()
        print()
        status()
        print()

    if phase in ("c", "all"):
        phase_c()
        print()
        status()

    if phase in ("d", "all", "de"):
        phase_d()
        print()
        status()

    if phase in ("e", "all", "de"):
        phase_e()
        print()
        status()

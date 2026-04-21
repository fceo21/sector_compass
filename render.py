"""
STEP 3: 섹터나침반 HTML 렌더러
analyze.py 결과 → 주간 HTML 리포트 생성

출력물:
- 이번 주 섹터 신호 카드 (tone 중심)
- 강세/경계 섹터 리포트 내용 요약
- 커버리지 히트맵 (8주 윈도우 + 좌우 네비게이션)
"""
import re
import sqlite3
from pathlib import Path
from datetime import date, timedelta
from analyze import build_weekly_stats, compute_indicators, get_all_weeks, week_monday, DB_PATH
from env_loader import get_key

OUTPUT_DIR = Path(__file__).parent

EXCLUDE_SECTORS = {'기타'}


# ──────────────────────────────────────────────────────────
# 주차 표시 변환
# ──────────────────────────────────────────────────────────

def iso_week_to_korean(iso_week_str: str) -> str:
    """'2026-W17' → '4월 4주'"""
    monday = week_monday(iso_week_str)
    week_in_month = (monday.day - 1) // 7 + 1
    return f"{monday.month}월 {week_in_month}주"


# ──────────────────────────────────────────────────────────
# 신호 분류
# ──────────────────────────────────────────────────────────

def classify_signal(r: dict) -> str:
    if r['count'] == 0 and r['dormancy_weeks'] >= 2:
        return 'DORMANT'
    if r['count'] == 0:
        return 'NEUTRAL'
    # 건수 ≥ 1, 톤 기준으로 분류
    if r['tone_ratio'] <= -0.1:
        return 'CAUTION'
    if r['tone_ratio'] >= 0.4:
        return 'STRONG'
    if r['tone_ratio'] >= 0.1:
        return 'POSITIVE'
    return 'NEUTRAL'


# 한국 증시 색상: 상승=빨강, 하락=파랑, 중립=회색
SIGNAL_META = {
    'STRONG':   {'label': '강세 🔴', 'cls': 'strong',   'color': '#d32f2f'},
    'POSITIVE': {'label': '긍정 🟥', 'cls': 'positive', 'color': '#e57373'},
    'CAUTION':  {'label': '경계 🔵', 'cls': 'caution',  'color': '#1565c0'},
    'NEUTRAL':  {'label': '관망 ⚪', 'cls': 'neutral',  'color': '#9e9e9e'},
    'DORMANT':  {'label': '침묵 ⚫', 'cls': 'dormant',  'color': '#616161'},
}


# ──────────────────────────────────────────────────────────
# Groq LLM 요약 제목 생성
# ──────────────────────────────────────────────────────────

TITLE_SYS = """당신은 한국 주식시장 리포트 제목 요약 전문가입니다.
각 리포트의 본문 요약을 보고, 핵심 투자 메시지를 담은 짧은 제목을 만드세요.

규칙:
- 반드시 번호: 제목 형식으로만 답하세요
- 20자 이내, 핵심 포인트 중심
- 다른 설명 일절 금지"""


def generate_report_titles(reports: list[dict]) -> dict:
    """Groq으로 요약 제목 생성. {0-based idx: 생성제목} 반환
    - 배치 10건씩 분할, TPM 초과 방지
    """
    import time as _time
    api_key = get_key("GROQ_API_KEY")
    if not api_key or not reports:
        return {}

    try:
        from groq import Groq
        client = Groq(api_key=api_key)
    except Exception as e:
        print(f"[제목 생성 오류] {e}")
        return {}

    BATCH = 10
    DELAY = 10  # 초 (6000 TPM 제한 준수)
    result = {}

    for batch_start in range(0, len(reports), BATCH):
        batch = reports[batch_start: batch_start + BATCH]
        lines = []
        for i, rp in enumerate(batch, 1):
            ab = (rp['abstract'] or '')[:100].replace('\n', ' ')
            lines.append(f"{i}. 원제: {rp['title'][:50]} / 요약: {ab}")
        prompt = (
            "다음 리포트들의 핵심을 담은 짧은 제목(20자 이내)을 만드세요.\n\n"
            + "\n".join(lines)
            + "\n\n답변 형식 (번호: 제목):"
        )
        try:
            resp = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": TITLE_SYS},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.3,
                max_tokens=400,
            )
            for line in resp.choices[0].message.content.splitlines():
                m = re.search(r'(\d+)\s*[:.\)]\s*(.+)', line)
                if m:
                    global_idx = batch_start + int(m.group(1)) - 1
                    result[global_idx] = m.group(2).strip()
        except Exception as e:
            print(f"[제목 생성 오류 배치 {batch_start}] {e}")

        if batch_start + BATCH < len(reports):
            _time.sleep(DELAY)

    return result


# ──────────────────────────────────────────────────────────
# 섹터별 리포트 내용 조회 (요약 섹션용)
# ──────────────────────────────────────────────────────────

def get_sector_reports(target_week: str, sectors: list) -> dict:
    """대상 주의 지정 섹터 리포트 목록 반환"""
    if not sectors:
        return {}
    monday = week_monday(target_week)
    sunday = monday + timedelta(days=6)
    placeholders = ','.join('?' * len(sectors))
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(f"""
        SELECT COALESCE(sector_refined, sector) AS sector,
               title, firm, tone, abstract, report_date, nid, pdf_url
        FROM raw_reports
        WHERE report_date >= ? AND report_date <= ?
          AND COALESCE(sector_refined, sector) IN ({placeholders})
        ORDER BY sector,
                 CASE tone WHEN '부정' THEN 0 WHEN '긍정' THEN 2 ELSE 1 END,
                 report_date DESC
    """, [monday.isoformat(), sunday.isoformat()] + list(sectors)).fetchall()
    conn.close()
    result = {}
    for sector, title, firm, tone, abstract, rdate, nid, pdf_url in rows:
        result.setdefault(sector, []).append({
            'title': title, 'firm': firm, 'tone': tone,
            'abstract': (abstract or '').strip(), 'date': rdate,
            'nid': nid, 'pdf_url': pdf_url or '',
        })
    return result


def make_ab_short(ab: str) -> str:
    """abstract → 2~3줄 요약 (160~280자)"""
    if not ab:
        return ''
    ab = ab.replace('\n', ' ').strip()
    if len(ab) <= 280:
        return ab
    # 280자 근처에서 자연스러운 문장 끝 찾기
    cut = ab[:280]
    for sep in ['다. ', '다.', '. ', '음. ']:
        pos = cut.rfind(sep)
        if pos > 120:
            return cut[:pos + len(sep)].rstrip() + '…'
    return cut + '…'


# ──────────────────────────────────────────────────────────
# HTML 조각 헬퍼
# ──────────────────────────────────────────────────────────

def fmt_tone(t: float) -> str:
    if t > 0.15:  return f'<span class="pos">{t:+.2f}</span>'
    if t < -0.15: return f'<span class="neg">{t:+.2f}</span>'
    return f'<span class="neu">{t:+.2f}</span>'


def tone_badge(tone: str) -> str:
    cls = {'긍정': 'pos', '부정': 'neg'}.get(tone, 'neu')
    return f'<span class="tbadge {cls}">{tone}</span>'


# ──────────────────────────────────────────────────────────
# 히트맵 (8주 윈도우 + JS 네비게이션)
# ──────────────────────────────────────────────────────────

def build_heatmap(weekly_stats: dict, all_weeks: list, all_sectors: list) -> str:
    """섹터 × 주차 히트맵 — 섹터는 전체 누적 건수 내림차순, 8주 윈도우 네비"""
    import json
    n_weeks = len(all_weeks)

    # 전체 누적 건수로 섹터 정렬
    sector_totals = {
        s: sum(weekly_stats.get(w, {}).get(s, {}).get('count', 0) for w in all_weeks)
        for s in all_sectors
    }
    sorted_sectors = sorted(all_sectors, key=lambda s: -sector_totals[s])

    max_count = max(
        weekly_stats.get(w, {}).get(s, {}).get('count', 0)
        for w in all_weeks for s in all_sectors
    ) or 1

    # 한국식 주차 레이블
    week_labels = [iso_week_to_korean(w) for w in all_weeks]

    # 헤더 행
    week_ths = "".join(
        f'<th class="wk-col" data-wi="{i}">{week_labels[i]}</th>'
        for i in range(n_weeks)
    )
    header = f'<tr><th class="sector-label">섹터</th>{week_ths}</tr>'

    # 데이터 행
    data_rows = []
    for sector in sorted_sectors:
        cells = []
        for i, wk in enumerate(all_weeks):
            cnt = weekly_stats.get(wk, {}).get(sector, {}).get('count', 0)
            td  = weekly_stats.get(wk, {}).get(sector, {})
            tp  = td.get('tone_pos', 0)
            tn  = td.get('tone_neg', 0)
            intensity = max(1, int(cnt / max_count * 9)) if cnt > 0 else 0
            if cnt > 0:
                if tp > tn:   ccls = f"h-pos-{intensity}"
                elif tn > tp: ccls = f"h-neg-{intensity}"
                else:         ccls = f"h-neu-{intensity}"
            else:
                ccls = "h-empty"
            cells.append(
                f'<td class="{ccls} wk-col" data-wi="{i}" '
                f'title="{week_labels[i]} {sector}: {cnt}건 (긍{tp}/부{tn})">'
                f'{cnt if cnt > 0 else ""}</td>'
            )
        data_rows.append(
            f'<tr><td class="sector-label">{sector}</td>{"".join(cells)}</tr>'
        )

    week_labels_json = json.dumps(week_labels, ensure_ascii=False)

    return f"""
<div class="heatmap-nav">
  <button id="hm-prev" onclick="hmShift(-1)">&#9664; 이전</button>
  <span id="hm-range"></span>
  <button id="hm-next" onclick="hmShift(1)">다음 &#9654;</button>
</div>
<div class="heatmap-wrap">
  <table class="heatmap" id="heatmap-tbl">
    <thead>{header}</thead>
    <tbody>{"".join(data_rows)}</tbody>
  </table>
  <div class="heatmap-legend">
    <span class="h-pos-5">■</span> 긍정톤 &nbsp;
    <span class="h-neg-5">■</span> 부정톤 &nbsp;
    <span class="h-neu-5">■</span> 중립톤 &nbsp;
    <span style="color:#3d4460">□</span> 발간없음
  </div>
</div>
<script>
(function(){{
  const N = {n_weeks};
  const WIN = 8;
  const weekLabels = {week_labels_json};
  let offset = Math.max(0, N - WIN);

  function update() {{
    document.querySelectorAll('.wk-col').forEach(el => {{
      const wi = parseInt(el.dataset.wi);
      el.style.display = (wi >= offset && wi < offset + WIN) ? '' : 'none';
    }});
    const lo = weekLabels[offset] || '';
    const hi = weekLabels[Math.min(offset + WIN - 1, N - 1)] || '';
    document.getElementById('hm-range').textContent = lo + ' ~ ' + hi;
    document.getElementById('hm-prev').disabled = offset <= 0;
    document.getElementById('hm-next').disabled = offset + WIN >= N;
  }}

  window.hmShift = function(dir) {{
    offset = Math.max(0, Math.min(offset + dir, N - WIN));
    update();
  }};

  update();
}})();
</script>"""


# ──────────────────────────────────────────────────────────
# 메인 렌더 함수
# ──────────────────────────────────────────────────────────

def render(target_week: str | None = None) -> Path:
    weeks       = get_all_weeks()
    stats       = build_weekly_stats()
    target_week = target_week or weeks[-1]
    indicators  = compute_indicators(stats, target_week)

    # 기타 제외
    indicators = [r for r in indicators if r['sector'] not in EXCLUDE_SECTORS]

    # 신호 분류
    for r in indicators:
        r['signal'] = classify_signal(r)

    signals = {k: [] for k in SIGNAL_META}
    for r in indicators:
        signals[r['signal']].append(r)

    for key in signals:
        signals[key].sort(key=lambda x: (-x['tone_ratio'], -x['count']))

    # 히트맵용 섹터 목록
    all_sectors = [r['sector'] for r in indicators]

    # ── 신호 카드 ───────────────────────────────────────────
    signal_cards = []
    for sig_key, meta in SIGNAL_META.items():
        items = signals[sig_key]
        # CAUTION은 없어도 칸 유지
        if not items and sig_key != 'CAUTION':
            continue
        rows_html = []
        if not items:
            rows_html.append('<li style="color:var(--muted);font-size:12px">이번 주 경계 신호 없음</li>')
        for r in items:
            firm_tooltip = ', '.join(r['firms']) if r['firms'] else ''
            tone_cls = 'pos' if r['tone_ratio'] > 0 else ('neg' if r['tone_ratio'] < 0 else 'neu')
            rows_html.append(
                f'<li>'
                f'<b>{r["sector"]}</b> '
                f'<span class="tone-score {tone_cls}">'
                f'{r["tone_ratio"]:+.2f}</span> '
                f'<span class="cnt-small" title="{firm_tooltip}">'
                f'{r["count"]}건/{r["firm_count"]}사</span>'
                f'{"&nbsp;💤" + str(r["dormancy_weeks"]) + "주" if r["dormancy_weeks"] > 0 and r["count"] == 0 else ""}'
                f'</li>'
            )
        signal_cards.append(f"""
        <div class="signal-card {meta['cls']}">
          <div class="sig-header">{meta['label']}</div>
          <ul>{"".join(rows_html)}</ul>
        </div>""")

    # ── 강세/경계 섹터 내용 요약 ────────────────────────────
    highlight_sectors = (
        [r['sector'] for r in signals['STRONG']] +
        [r['sector'] for r in signals['CAUTION']]
    )
    sector_reports = get_sector_reports(target_week, highlight_sectors)

    # 신호별 톤 정렬: STRONG → 긍정 우선, CAUTION → 부정 우선
    def sort_rpts_for_signal(rpts: list, sig_key: str) -> list:
        if sig_key == 'STRONG':
            order = {'긍정': 0, '중립': 1, '부정': 2}
        else:
            order = {'부정': 0, '중립': 1, '긍정': 2}
        return sorted(rpts, key=lambda rp: order.get(rp['tone'], 1))

    # 정렬된 리포트 미리 준비 (LLM 인덱스 일치 위해)
    sorted_sector_reports: dict[str, dict] = {}
    all_highlight_rpts = []
    for sig_key in ('STRONG', 'CAUTION'):
        sorted_sector_reports[sig_key] = {}
        for r in signals[sig_key]:
            rpts = sort_rpts_for_signal(sector_reports.get(r['sector'], []), sig_key)
            sorted_sector_reports[sig_key][r['sector']] = rpts
            all_highlight_rpts.extend(rpts)

    print(f"[제목 생성] 대상 {len(all_highlight_rpts)}건...")
    gen_titles = generate_report_titles(all_highlight_rpts)
    print(f"[제목 생성] 완료 {len(gen_titles)}건")

    # 전역 인덱스 추적용
    rpt_global_idx = [0]

    summary_blocks = []
    for sig_key in ('STRONG', 'CAUTION'):
        for r in signals[sig_key]:
            sec = r['sector']
            rpts = sorted_sector_reports[sig_key].get(sec, [])
            if not rpts:
                continue
            meta = SIGNAL_META[sig_key]
            items_html = []
            for rp in rpts:
                idx = rpt_global_idx[0]
                rpt_global_idx[0] += 1

                ab_short = make_ab_short(rp['abstract'])

                # gen_title: LLM 생성 제목, 앞에 붙는 "N:" 패턴 제거
                gen_title = gen_titles.get(idx, '')
                if gen_title:
                    gen_title = re.sub(r'^\d+\s*[:\.]\s*', '', gen_title).strip()

                # 원문 링크 — pdf_url 우선, 없으면 Naver 페이지, gen_title에만 링크
                pdf_url = rp.get('pdf_url', '')
                nid = rp.get('nid')
                if pdf_url:
                    report_url = pdf_url
                elif nid:
                    report_url = f"https://finance.naver.com/research/industry_read.naver?nid={nid}"
                else:
                    report_url = None
                if gen_title:
                    if report_url:
                        title_html = (
                            f'<a href="{report_url}" target="_blank" class="rpt-link">{gen_title}</a>'
                            f' <span class="orig-title">({rp["title"]})</span>'
                        )
                    else:
                        title_html = f'{gen_title} <span class="orig-title">({rp["title"]})</span>'
                else:
                    title_html = (
                        f'<a href="{report_url}" target="_blank" class="rpt-link">{rp["title"]}</a>'
                        if report_url else rp["title"]
                    )

                items_html.append(
                    f'<div class="rpt-item">'
                    f'<div class="rpt-meta">'
                    f'{tone_badge(rp["tone"])} '
                    f'<span class="rpt-firm">{rp["firm"]}</span>'
                    f'</div>'
                    f'<div class="rpt-title">{title_html}</div>'
                    f'{"<div class=rpt-abs>" + ab_short + "</div>" if ab_short else ""}'
                    f'</div>'
                )
            summary_blocks.append(f"""
        <div class="summary-block {meta['cls']}">
          <div class="sum-header">
            <span class="sum-sector">{sec}</span>
            <span class="sig-badge-sm {meta['cls']}">{meta['label']}</span>
            <span class="sum-count">{r['count']}건 · {r['firm_count']}사 · {fmt_tone(r['tone_ratio'])}</span>
          </div>
          <div class="rpt-list">{"".join(items_html)}</div>
        </div>""")

    summary_html = "".join(summary_blocks)

    # ── 히트맵 ─────────────────────────────────────────────
    heatmap_html = build_heatmap(stats, weeks, all_sectors)

    # ── 통계 수치 ───────────────────────────────────────────
    total_this_week = sum(r['count'] for r in indicators)
    active_sectors  = sum(1 for r in indicators if r['count'] > 0)
    strong_count    = len(signals['STRONG'])
    positive_count  = len(signals['POSITIVE'])
    caution_count   = len(signals['CAUTION'])
    dormant_count   = len(signals['DORMANT'])
    today           = date.today().isoformat()
    week_korean     = iso_week_to_korean(target_week)

    # ── 이전/다음주 네비 ────────────────────────────────────
    tw_idx     = weeks.index(target_week)
    prev_week  = weeks[tw_idx - 1] if tw_idx > 0 else None
    next_week  = weeks[tw_idx + 1] if tw_idx < len(weeks) - 1 else None
    prev_exists = prev_week and (OUTPUT_DIR / f"compass_{prev_week}.html").exists()
    next_exists = next_week and (OUTPUT_DIR / f"compass_{next_week}.html").exists()

    prev_btn = (
        f'<a href="compass_{prev_week}.html" class="week-nav-btn">'
        f'&#9664; {iso_week_to_korean(prev_week)}</a>'
        if prev_exists else
        '<span class="week-nav-btn disabled">&#9664;</span>'
    )
    next_btn = (
        f'<a href="compass_{next_week}.html" class="week-nav-btn">'
        f'{iso_week_to_korean(next_week)} &#9654;</a>'
        if next_exists else
        '<span class="week-nav-btn disabled">&#9654;</span>'
    )

    # ── HTML 조립 ───────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>K-FIN 섹터나침반 {week_korean}</title>
<style>
  :root {{
    --bg: #0f1117; --card: #1a1d27; --border: #2d3147;
    --text: #e0e4f0; --muted: #7a8099; --accent: #e03020;
    --pos: #e03020; --neg: #1565c0; --neu: #9e9e9e;
    --strong-c:   #d32f2f;
    --positive-c: #e57373;
    --caution-c:  #1565c0;
    --neutral-c:  #9e9e9e;
    --dormant-c:  #616161;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text);
          font-family: 'Segoe UI','Noto Sans KR',sans-serif; font-size:14px; line-height:1.6; }}
  .container {{ max-width:1400px; margin:0 auto; padding:24px; }}

  /* 헤더 */
  .header {{ margin-bottom:28px; border-bottom:1px solid var(--border); padding-bottom:16px; }}
  .header h1 {{ font-size:24px; font-weight:700; letter-spacing:-0.5px; display:flex; align-items:center; gap:12px; flex-wrap:wrap; }}
  .header h1 .brand {{ color:var(--accent); }}
  .header h1 .week  {{ color:var(--text); }}
  .week-nav-btn {{ font-size:12px; padding:3px 10px; border-radius:5px; border:1px solid var(--border);
                   background:var(--card); color:var(--text); text-decoration:none; white-space:nowrap; }}
  .week-nav-btn:hover {{ background:#222536; }}
  .week-nav-btn.disabled {{ color:var(--muted); cursor:default; opacity:.4; pointer-events:none; }}
  .subtitle {{ color:var(--muted); font-size:12px; margin-top:4px; }}
  .stats-row {{ display:flex; gap:16px; margin-top:16px; flex-wrap:wrap; }}
  .rpt-link {{ color:inherit; text-decoration:none; }}
  .rpt-link:hover {{ text-decoration:underline; color:var(--accent); }}
  .stat-box {{ background:var(--card); border:1px solid var(--border); border-radius:8px; padding:12px 20px; }}
  .stat-box .label {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.5px; }}
  .stat-box .value {{ font-size:22px; font-weight:700; margin-top:2px; }}

  /* 신호 카드 */
  .section-title {{ font-size:16px; font-weight:600; margin-bottom:12px; color:var(--text); }}
  .signal-grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(200px,1fr)); gap:14px; margin-bottom:28px; }}
  .signal-card {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:14px; }}
  .signal-card.strong   {{ border-top:3px solid var(--strong-c); }}
  .signal-card.positive {{ border-top:3px solid var(--positive-c); }}
  .signal-card.caution  {{ border-top:3px solid var(--caution-c); }}
  .signal-card.neutral  {{ border-top:3px solid var(--neutral-c); }}
  .signal-card.dormant  {{ border-top:3px solid var(--dormant-c); }}
  .sig-header {{ font-weight:700; font-size:15px; margin-bottom:9px; }}
  .strong   .sig-header {{ color:var(--strong-c); }}
  .positive .sig-header {{ color:var(--positive-c); }}
  .caution  .sig-header {{ color:var(--caution-c); }}
  .neutral  .sig-header {{ color:var(--neutral-c); }}
  .dormant  .sig-header {{ color:var(--dormant-c); }}
  .signal-card ul {{ list-style:none; }}
  .signal-card li {{ padding:5px 0; border-bottom:1px solid var(--border); font-size:13px; }}
  .signal-card li:last-child {{ border:none; }}
  .tone-score {{ font-weight:700; font-size:12px; }}
  .tone-score.pos {{ color:var(--pos); }}
  .tone-score.neg {{ color:var(--neg); }}
  .tone-score.neu {{ color:var(--neu); }}
  .cnt-small {{ color:var(--muted); font-size:11px; cursor:default;
                border-bottom:1px dotted var(--muted); }}

  /* 섹터 요약 */
  .summary-section {{ margin-bottom:36px; }}
  .summary-block {{ background:var(--card); border:1px solid var(--border);
                    border-radius:10px; margin-bottom:14px; overflow:hidden; }}
  .summary-block.strong  {{ border-left:4px solid var(--strong-c); }}
  .summary-block.positive{{ border-left:4px solid var(--positive-c); }}
  .summary-block.caution {{ border-left:4px solid var(--caution-c); }}
  .sum-header {{ display:flex; align-items:center; gap:10px; padding:12px 16px;
                 border-bottom:1px solid var(--border); }}
  .sum-sector {{ font-size:16px; font-weight:700; }}
  .sig-badge-sm {{ font-size:11px; font-weight:700; padding:2px 8px; border-radius:4px; }}
  .strong  .sig-badge-sm {{ background:rgba(211,47,47,.15);  color:var(--strong-c); }}
  .positive .sig-badge-sm {{ background:rgba(229,115,115,.15); color:var(--positive-c); }}
  .caution .sig-badge-sm {{ background:rgba(21,101,192,.15); color:var(--caution-c); }}
  .sum-count {{ color:var(--muted); font-size:12px; margin-left:auto; }}
  .rpt-list {{ padding:12px 16px; display:flex; flex-direction:column; gap:10px; }}
  .rpt-item {{ padding:8px 10px; background:#151820; border-radius:6px; }}
  .rpt-meta {{ display:flex; align-items:center; gap:8px; margin-bottom:4px; }}
  .rpt-firm {{ color:var(--muted); font-size:11px; }}
  .rpt-title {{ font-weight:600; font-size:13px; margin-bottom:4px; }}
  .orig-title {{ font-weight:400; font-size:11px; color:var(--muted); }}
  .rpt-abs {{ color:#c0c8e0; font-size:12px; line-height:1.65; }}
  .tbadge {{ font-size:10px; font-weight:700; padding:1px 6px; border-radius:3px; }}
  .tbadge.pos {{ background:rgba(224,48,32,.2);  color:var(--pos); }}
  .tbadge.neg {{ background:rgba(21,101,192,.2); color:var(--neg); }}
  .tbadge.neu {{ background:rgba(158,158,158,.2); color:var(--neu); }}
  .pos {{ color:var(--pos); }} .neg {{ color:var(--neg); }} .neu {{ color:var(--neu); }}

  /* 히트맵 */
  .heatmap-nav {{ display:flex; align-items:center; gap:12px; margin-bottom:10px; }}
  .heatmap-nav button {{ background:var(--card); border:1px solid var(--border); color:var(--text);
                         padding:5px 14px; border-radius:6px; cursor:pointer; font-size:13px; }}
  .heatmap-nav button:hover {{ background:#222536; }}
  .heatmap-nav button:disabled {{ opacity:.3; cursor:default; }}
  #hm-range {{ color:var(--muted); font-size:13px; }}
  .heatmap-wrap {{ overflow-x:auto; margin-bottom:8px; }}
  .heatmap {{ border-collapse:collapse; font-size:11px; white-space:nowrap; }}
  .heatmap th, .heatmap td {{ padding:4px 7px; border:1px solid var(--border); text-align:center; }}
  .heatmap th {{ background:var(--card); color:var(--muted); }}
  .sector-label {{ text-align:left !important; padding-left:10px !important; font-weight:600;
                   white-space:nowrap; min-width:90px; }}
  .h-empty {{ background:#1a1d27; color:transparent; }}
  /* 긍정 → 빨강 계열 */
  .h-pos-1{{background:#2a1818}} .h-pos-2{{background:#3d1f1f}} .h-pos-3{{background:#561f1f}}
  .h-pos-4{{background:#742020}} .h-pos-5{{background:#922222}} .h-pos-6{{background:#b02525;color:#fff}}
  .h-pos-7{{background:#c83030;color:#fff}} .h-pos-8{{background:#e03838;color:#fff}}
  .h-pos-9{{background:#ff4040;color:#fff;font-weight:700}}
  /* 부정 → 파랑 계열 */
  .h-neg-1{{background:#181a2a}} .h-neg-2{{background:#1a2040}} .h-neg-3{{background:#1a2860}}
  .h-neg-4{{background:#1a3080}} .h-neg-5{{background:#183ea0}} .h-neg-6{{background:#154ebe;color:#fff}}
  .h-neg-7{{background:#1260d8;color:#fff}} .h-neg-8{{background:#1070ee;color:#fff}}
  .h-neg-9{{background:#1a80ff;color:#fff;font-weight:700}}
  /* 중립 → 회색 계열 */
  .h-neu-1{{background:#1e1e20}} .h-neu-2{{background:#252528}} .h-neu-3{{background:#2d2d32}}
  .h-neu-4{{background:#383840}} .h-neu-5{{background:#46464e}} .h-neu-6{{background:#57575f;color:#ccc}}
  .h-neu-7{{background:#686870;color:#fff}} .h-neu-8{{background:#787882;color:#fff}}
  .h-neu-9{{background:#888892;color:#fff;font-weight:700}}
  .heatmap-legend {{ font-size:11px; color:var(--muted); margin-top:6px; }}
  .heatmap-legend span {{ margin-right:10px; }}

  /* 푸터 */
  .footer {{ color:var(--muted); font-size:11px; border-top:1px solid var(--border);
             padding-top:14px; margin-top:28px; }}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>🧭 <span class="brand">K-FIN</span> 섹터나침반 <span class="week">{week_korean}</span>
      {prev_btn}
      {next_btn}
    </h1>
    <div class="subtitle">생성일: {today}</div>
    <div class="stats-row">
      <div class="stat-box">
        <div class="label">이번 주 리포트</div>
        <div class="value">{total_this_week}건</div>
      </div>
      <div class="stat-box">
        <div class="label">활성 섹터</div>
        <div class="value">{active_sectors}개</div>
      </div>
      <div class="stat-box">
        <div class="label">강세 섹터</div>
        <div class="value" style="color:var(--strong-c)">{strong_count}개</div>
      </div>
      <div class="stat-box">
        <div class="label">긍정 섹터</div>
        <div class="value" style="color:var(--positive-c)">{positive_count}개</div>
      </div>
      <div class="stat-box">
        <div class="label">경계 섹터</div>
        <div class="value" style="color:var(--caution-c)">{caution_count}개</div>
      </div>
      <div class="stat-box">
        <div class="label">침묵 섹터</div>
        <div class="value" style="color:var(--dormant-c)">{dormant_count}개</div>
      </div>
    </div>
  </div>

  <div class="section-title">이번 주 섹터 신호</div>
  <div class="signal-grid">{"".join(signal_cards)}</div>

  <div class="section-title">강세 · 경계 섹터 리포트</div>
  <div class="summary-section">{summary_html if summary_html else '<p style="color:var(--muted)">해당 신호 없음</p>'}</div>

  <div class="section-title">커버리지 히트맵 ({iso_week_to_korean(weeks[0])} ~ {iso_week_to_korean(weeks[-1])})</div>
  {heatmap_html}

  <div class="footer">
    데이터 출처: Naver Finance 산업리포트 &nbsp;|&nbsp; K-FIN 섹터나침반 v1.3
  </div>

</div>
</body>
</html>"""

    out_path = OUTPUT_DIR / f"compass_{target_week}.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"[render] 저장: {out_path}")
    return out_path


if __name__ == "__main__":
    import sys
    week = sys.argv[1] if len(sys.argv) > 1 else None
    path = render(week)
    print(f"완료: {path}")

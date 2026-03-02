"""
Portal Data Sekolah — Production Build v5.0
Optimized for 50+ concurrent users on Streamlit Cloud.

Optimisation strategy
─────────────────────
1.  st.cache_resource  → server-level singleton shared by ALL users.
    Spreadsheet is downloaded & parsed exactly ONCE per URL, regardless
    of how many people are using the app simultaneously.

2.  requests + BytesIO  → no temp-file I/O, no disk pressure.

3.  ThreadPoolExecutor  → sheets parsed in parallel (up to 8 threads).

4.  Category dtype      → 50–70 % RAM reduction on low-cardinality columns.

5.  O(1) NPSN index     → dict-of-positions built at load time;
    search never scans the DataFrame.

6.  Server-side HTML table → no Arrow / pyarrow serialisation overhead;
    result cached per (url_hash, group, theme) in session_state.

7.  No time.sleep / st.rerun polling → critical for concurrency;
    polling blocks the Streamlit thread and starves other users.

8.  Per-user session_state holds ONLY tiny primitives (strings, dicts of
    cached HTML strings). Heavy objects live in cache_resource.
"""

from __future__ import annotations

import hashlib
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

# ──────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Portal Data Sekolah", layout="wide")

# ──────────────────────────────────────────────────────────────
# SESSION DEFAULTS  (per-user, lightweight primitives only)
# ──────────────────────────────────────────────────────────────
_DEFAULTS: dict = {
    "dark_mode":      False,
    "active_url":     None,
    "active_hash":    None,
    "last_q":         "",
    "html_tbl_cache": {},   # {(url_hash, grp, dark): html_str}
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

DM: bool = st.session_state.dark_mode


# ──────────────────────────────────────────────────────────────
# THEME
# ──────────────────────────────────────────────────────────────
def _theme(dark: bool) -> dict:
    return {
        "bg":     "#0f172a" if dark else "#f0f4ff",
        "surf":   "#1e293b" if dark else "#ffffff",
        "surf2":  "#273449" if dark else "#eef2ff",
        "bdr":    "#334155" if dark else "#c7d2fe",
        "txt":    "#f1f5f9" if dark else "#1e1b4b",
        "txt2":   "#94a3b8" if dark else "#4338ca",
        "txt3":   "#64748b" if dark else "#818cf8",
        "acc":    "#6366f1" if dark else "#4f46e5",
        "acc2":   "#818cf8" if dark else "#6366f1",
        "inp":    "#0f172a" if dark else "#f8faff",
        "g1":     "#3730a3" if dark else "#4f46e5",
        "g2":     "#6366f1" if dark else "#818cf8",
        "sb_bg":  "#1e293b" if dark else "#ffffff",
        "sb_bdr": "#334155" if dark else "#c7d2fe",
        "st_b":   "#1e3a5f" if dark else "#e0e7ff",
        "st_g":   "#14532d" if dark else "#d1fae5",
        "st_p":   "#3b0764" if dark else "#ede9fe",
        "row_a":  "#273449" if dark else "#f5f7ff",
        "row_h":  "#1e3a5f" if dark else "#eef2ff",
        "th_bg":  "#111827" if dark else "#e0e7ff",
        "th_c":   "#818cf8" if dark else "#4338ca",
        "tbl_bg": "#1e293b" if dark else "#ffffff",
        "td_bdr": "#334155" if dark else "#c7d2fe",
        "td_txt": "#f1f5f9" if dark else "#1e1b4b",
        "ntf_bg": "rgba(16,185,129,.12)" if dark else "#ecfdf5",
        "ntf_t":  "#34d399" if dark else "#059669",
        "ntf_d":  "#6ee7b7" if dark else "#065f46",
        "ntf_b":  "rgba(16,185,129,.2)" if dark else "#a7f3d0",
        "ntf_bc": "#34d399" if dark else "#065f46",
    }

T = _theme(DM)


# ──────────────────────────────────────────────────────────────
# CSS  (injected once; st.html is a no-op if content unchanged)
# ──────────────────────────────────────────────────────────────
st.html(f"""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box}}
body,.stApp{{background:{T['bg']};font-family:'Space Grotesk',sans-serif;color:{T['txt']}}}
::-webkit-scrollbar{{width:4px;height:4px}}
::-webkit-scrollbar-thumb{{background:{T['acc']};border-radius:2px}}
[data-testid="stSidebar"]{{background:{T['sb_bg']}!important;border-right:1px solid {T['sb_bdr']}!important}}
[data-testid="stSidebar"]>div{{padding:14px 12px!important}}

.hdr{{display:flex;align-items:center;gap:14px;padding:18px 24px;
  background:linear-gradient(135deg,{T['g1']},{T['g2']});border-radius:14px;
  margin-bottom:20px;box-shadow:0 6px 24px rgba(99,102,241,.3)}}
.hdr-title{{font-size:20px;font-weight:700;color:#fff;letter-spacing:-.2px}}
.hdr-sub{{font-size:11px;color:rgba(255,255,255,.6);margin-top:2px}}
.hdr-badge{{margin-left:auto;font-family:'JetBrains Mono',monospace;font-size:10px;
  color:#fff;border:1px solid rgba(255,255,255,.3);padding:4px 10px;
  border-radius:20px;background:rgba(255,255,255,.12)}}

.stats{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px}}
.stat{{background:{T['surf']};border:1px solid {T['bdr']};border-radius:12px;
  padding:16px 18px;display:flex;align-items:center;gap:12px}}
.stat-ico{{width:42px;height:42px;border-radius:10px;display:flex;align-items:center;
  justify-content:center;font-size:18px;flex-shrink:0}}
.stat-ico.b{{background:{T['st_b']}}} .stat-ico.g{{background:{T['st_g']}}} .stat-ico.p{{background:{T['st_p']}}}
.stat-lbl{{font-size:10px;text-transform:uppercase;letter-spacing:.8px;
  color:{T['txt3']};font-family:'JetBrains Mono',monospace}}
.stat-val{{font-family:'JetBrains Mono',monospace;font-size:24px;font-weight:600;color:{T['txt']}}}
.stat-sub{{font-size:11px;color:{T['txt3']};margin-top:1px}}

.plbl{{font-family:'JetBrains Mono',monospace;font-size:10px;text-transform:uppercase;
  letter-spacing:1px;color:{T['txt2']};margin-bottom:8px;display:flex;align-items:center;gap:6px}}
.plbl-bar{{width:3px;height:12px;background:{T['acc']};border-radius:2px;display:inline-block}}

.sync{{display:flex;align-items:center;gap:8px;font-family:'JetBrains Mono',monospace;
  font-size:11px;color:{T['txt2']};padding:8px 14px;margin-bottom:16px;
  background:{T['surf']};border:1px solid {T['bdr']};border-radius:8px}}
.sync-dot{{width:7px;height:7px;border-radius:50%;background:#10b981;
  animation:pulse 2s infinite;flex-shrink:0}}
@keyframes pulse{{0%,100%{{box-shadow:0 0 0 2px rgba(16,185,129,.2)}}50%{{box-shadow:0 0 0 5px rgba(16,185,129,.05)}}}}

div[data-testid="stForm"]{{background:transparent!important;border:none!important;padding:0!important}}
.stTextInput>div>div>input{{background:{T['inp']}!important;border:2px solid {T['bdr']}!important;
  color:{T['txt']}!important;border-radius:10px!important;
  font-family:'JetBrains Mono',monospace!important;font-size:13px!important;padding:10px 14px!important}}
.stTextInput>div>div>input:focus{{border-color:{T['acc']}!important;
  box-shadow:0 0 0 3px rgba(99,102,241,.15)!important}}
.stTextInput>label{{color:{T['txt2']}!important;font-size:10px!important;
  font-family:'JetBrains Mono',monospace!important;text-transform:uppercase!important;letter-spacing:.8px!important}}

.stButton>button,.stFormSubmitButton>button{{
  background:linear-gradient(135deg,{T['g1']},{T['g2']})!important;color:#fff!important;
  border:none!important;border-radius:8px!important;
  font-family:'JetBrains Mono',monospace!important;font-size:11px!important;
  font-weight:600!important;padding:9px 16px!important;transition:all .15s!important;
  box-shadow:0 2px 8px rgba(99,102,241,.25)!important}}
.stButton>button:hover,.stFormSubmitButton>button:hover{{
  transform:translateY(-1px)!important;box-shadow:0 4px 14px rgba(99,102,241,.35)!important}}

.rcard{{background:{T['surf']};border:1px solid {T['bdr']};border-radius:12px;overflow:hidden;margin-bottom:16px}}
.rcard-hdr{{display:flex;align-items:center;gap:8px;padding:12px 16px;
  background:{T['surf2']};border-bottom:2px solid {T['acc']}}}
.rcard-title{{font-size:13px;font-weight:600;color:{T['txt']}}}
.rcard-badge{{font-size:10px;background:rgba(99,102,241,.15);color:{T['acc2']};
  border:1px solid rgba(99,102,241,.25);padding:2px 9px;border-radius:10px;font-weight:600}}
.rcard-sheet{{margin-left:auto;font-size:10px;color:{T['txt3']};font-family:'JetBrains Mono',monospace}}

.notif{{background:{T['ntf_bg']};border:2px solid #10b981;border-radius:12px;
  padding:14px 18px;margin-bottom:16px;display:flex;align-items:center;gap:12px;
  animation:nin .35s cubic-bezier(.34,1.5,.64,1)}}
@keyframes nin{{from{{transform:translateY(-8px);opacity:0}}to{{transform:translateY(0);opacity:1}}}}
.notif-title{{font-weight:700;font-size:14px;color:{T['ntf_t']}}}
.notif-detail{{font-size:11px;color:{T['ntf_d']};font-family:'JetBrains Mono',monospace;margin-top:2px}}
.notif-badge{{margin-left:auto;background:{T['ntf_b']};color:{T['ntf_bc']};padding:5px 12px;
  border-radius:16px;font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;white-space:nowrap}}

.nf{{background:{T['surf']};border:2px dashed {T['bdr']};border-radius:12px;
  padding:28px;text-align:center;color:{T['txt3']}}}
.nf h4{{color:{T['txt']};font-size:13px;margin-bottom:4px;font-weight:600}}
.nf p{{font-size:11px}}

.dm-wrap{{display:flex;align-items:center;justify-content:space-between;
  padding:8px 12px;background:{T['surf2']};border:1px solid {T['bdr']};
  border-radius:8px;margin-bottom:10px}}
.dm-lbl{{font-family:'JetBrains Mono',monospace;font-size:11px;color:{T['txt2']};font-weight:600}}
hr{{border:none;border-top:1px solid {T['bdr']};margin:12px 0}}
</style>
""")


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _export_url(url: str) -> str:
    if "docs.google.com" not in url:
        return url
    try:
        sid = url.split("/d/")[1].split("/")[0]
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx"
    except Exception:
        return url


def _parse_sheet(excel: pd.ExcelFile, sheet_name: str) -> pd.DataFrame | None:
    try:
        raw = pd.read_excel(excel, sheet_name=sheet_name, header=None, dtype=str)
    except Exception:
        return None
    for i in range(min(15, len(raw))):
        if any("npsn" in v for v in raw.iloc[i].fillna("").str.lower().tolist()):
            df = raw.iloc[i + 1:].copy()
            df.columns = (
                raw.iloc[i].fillna("").str.lower()
                .str.strip().str.replace(r"\s+", "_", regex=True)
            )
            for c in df.columns:
                if "npsn" in c:
                    df = df.rename(columns={c: "npsn"})
                    break
            if "npsn" not in df.columns:
                return None
            df["source_sheet"] = sheet_name
            return df.dropna(how="all").reset_index(drop=True)
    return None


def _load_spreadsheet(clean_url: str) -> tuple[pd.DataFrame, dict, dict]:
    """Download + parallel-parse. Returns (data, npsn_index, meta)."""
    resp = requests.get(
        clean_url, timeout=45,
        headers={"User-Agent": "Mozilla/5.0 (compatible; PortalSekolah/5.0)"}
    )
    resp.raise_for_status()

    excel = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl")
    results: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=min(8, len(excel.sheet_names))) as pool:
        futures = {pool.submit(_parse_sheet, excel, s): s for s in excel.sheet_names}
        for fut in as_completed(futures):
            r = fut.result()
            if r is not None:
                results.append(r)

    if not results:
        return pd.DataFrame(), {}, {}

    data = pd.concat(results, ignore_index=True)

    # Low-cardinality columns → category (big RAM win for string columns)
    n = len(data)
    for col in data.select_dtypes(include="object").columns:
        try:
            if n == 0 or data[col].nunique() / n < 0.6:
                data[col] = data[col].astype("category")
        except Exception:
            pass

    # Build O(1) lookup index
    npsn_raw = data["npsn"].astype(str).str.strip()
    idx_map: dict[str, list[int]] = {}
    for pos, val in enumerate(npsn_raw):
        key = val.split("_")[0]
        idx_map.setdefault(key, []).append(pos)

    meta = {
        "n_rows":    n,
        "n_school":  data["npsn"].astype(str).str.split("_").str[0].nunique() if n else 0,
        "n_sheets":  data["source_sheet"].nunique() if n else 0,
    }
    return data, idx_map, meta


# ── Server-level shared cache ────────────────────────────────
# max_entries=8 → at most 8 different spreadsheets in memory at once.
# All 50+ users hitting the same URL share ONE cached object.
@st.cache_resource(max_entries=8)
def _server_store(url_hash: str) -> dict:
    """Mutable dict populated lazily on first access per URL."""
    return {"data": None, "index": None, "meta": None, "loaded_at": None}


def _ensure_loaded(url_hash: str, clean_url: str) -> dict:
    store = _server_store(url_hash)
    if store["data"] is None:
        with st.spinner("⏳ Memuat data spreadsheet..."):
            data, idx, meta = _load_spreadsheet(clean_url)
        store["data"]      = data
        store["index"]     = idx
        store["meta"]      = meta
        store["loaded_at"] = datetime.now().strftime("%H:%M:%S")
    return store


# ── HTML table builder ───────────────────────────────────────
def _build_table_html(df: pd.DataFrame, dark: bool) -> str:
    t = _theme(dark)
    bg, bdr, txt = t["tbl_bg"], t["td_bdr"], t["td_txt"]
    th_bg, th_c  = t["th_bg"], t["th_c"]
    row_a, row_h = t["row_a"], t["row_h"]
    acc          = t["acc"]

    ths = "".join(
        f'<th style="padding:8px 10px;background:{th_bg};color:{th_c};font-size:10px;'
        f'font-family:JetBrains Mono,monospace;text-transform:uppercase;letter-spacing:.8px;'
        f'font-weight:600;border-bottom:2px solid {acc};white-space:nowrap;text-align:left">'
        f'{c.replace("_", " ")}</th>'
        for c in df.columns
    )
    rows = []
    records = df.astype(str).replace("nan", "").to_dict("records")
    for i, r in enumerate(records):
        bg_row = bg if i % 2 == 0 else row_a
        tds = "".join(
            f'<td style="padding:7px 10px;font-size:12px;color:{txt};'
            f'border-bottom:1px solid {bdr};word-break:break-word;'
            f'white-space:normal;vertical-align:top;max-width:200px">{v}</td>'
            for v in r.values()
        )
        rows.append(
            f'<tr style="background:{bg_row}"'
            f' onmouseover="this.style.background=\'{row_h}\'"'
            f' onmouseout="this.style.background=\'{bg_row}\'">{tds}</tr>'
        )
    return (
        f'<div style="overflow:auto;width:100%">'
        f'<table style="width:100%;border-collapse:collapse;background:{bg}">'
        f'<thead><tr>{ths}</tr></thead><tbody>{"".join(rows)}</tbody>'
        f'</table></div>'
    )


# ──────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div class="dm-wrap">
      <span class="dm-lbl">{"🌙 Dark" if DM else "☀️ Light"} Mode</span>
    </div>""", unsafe_allow_html=True)
    col_l, col_r = st.columns(2)
    with col_l:
        if st.button("☀️ Light", key="btn_light", use_container_width=True):
            st.session_state.dark_mode = False
            st.rerun()
    with col_r:
        if st.button("🌙 Dark", key="btn_dark", use_container_width=True):
            st.session_state.dark_mode = True
            st.rerun()

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="font-family:'JetBrains Mono',monospace;font-size:10px;
      color:{T['txt3']};line-height:2">
      <b style="color:{T['txt2']}">Portal Data Sekolah</b><br>
      NPSN Lookup v5.0<br>
      Cache: server-shared ✅<br>
      50+ user: optimized ✅<br>
      Auto-update: klik Load
    </div>""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
# HEADER
# ──────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="hdr">
  <div style="font-size:30px">🏫</div>
  <div>
    <div class="hdr-title">Portal Data Sekolah</div>
    <div class="hdr-sub">Pencarian instalasi berbasis NPSN — cepat &amp; akurat</div>
  </div>
  <span class="hdr-badge">NPSN LOOKUP v5.0</span>
</div>""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
# FORM LOAD
# ──────────────────────────────────────────────────────────────
st.markdown('<div class="plbl"><span class="plbl-bar"></span>📂 Sumber Data</div>',
            unsafe_allow_html=True)
with st.form("load_form"):
    url_input = st.text_input(
        "Link Google Spreadsheet",
        placeholder="https://docs.google.com/spreadsheets/d/...",
        value=st.session_state.active_url or "",
    )
    load_btn = st.form_submit_button("▶  Load / Refresh Data")

if load_btn and url_input.strip():
    new_clean = _export_url(url_input.strip())
    new_hash  = _url_hash(new_clean)
    # Invalidate server store so data is re-fetched
    store = _server_store(new_hash)
    store["data"] = None
    st.session_state.active_url     = url_input.strip()
    st.session_state.active_hash    = new_hash
    st.session_state.last_q         = ""
    st.session_state.html_tbl_cache = {}
    st.rerun()


# ──────────────────────────────────────────────────────────────
# MAIN DATA AREA
# ──────────────────────────────────────────────────────────────
if st.session_state.active_url:
    clean_url = _export_url(st.session_state.active_url)
    url_hash  = st.session_state.active_hash or _url_hash(clean_url)

    store     = _ensure_loaded(url_hash, clean_url)
    data: pd.DataFrame      = store["data"]
    idx_map: dict           = store["index"]
    meta: dict              = store["meta"]
    loaded_at: str          = store["loaded_at"] or "-"

    # Sync bar
    st.markdown(f"""
    <div class="sync">
      <span class="sync-dot"></span>
      <span>DATA AKTIF &nbsp;—&nbsp; Dimuat: <b>{loaded_at}</b></span>
      &nbsp;|&nbsp;
      <span style="color:{T['txt3']}">Klik <b>Load / Refresh</b> untuk update terbaru</span>
    </div>""", unsafe_allow_html=True)

    # Stat cards
    st.markdown(f"""
    <div class="stats">
      <div class="stat"><div class="stat-ico b">📋</div>
        <div><div class="stat-lbl">Total Baris</div>
        <div class="stat-val">{meta['n_rows']:,}</div>
        <div class="stat-sub">semua sheet</div></div></div>
      <div class="stat"><div class="stat-ico g">🏫</div>
        <div><div class="stat-lbl">Total Sekolah</div>
        <div class="stat-val">{meta['n_school']:,}</div>
        <div class="stat-sub">unique NPSN</div></div></div>
      <div class="stat"><div class="stat-ico p">📑</div>
        <div><div class="stat-lbl">Sheet Aktif</div>
        <div class="stat-val">{meta['n_sheets']}</div>
        <div class="stat-sub">berkolom NPSN</div></div></div>
    </div>""", unsafe_allow_html=True)

    # Search
    st.markdown('<div class="plbl"><span class="plbl-bar"></span>🔍 Cari Data NPSN</div>',
                unsafe_allow_html=True)
    q = st.text_input(
        "npsn_q",
        placeholder="Masukkan NPSN lalu Enter...",
        label_visibility="collapsed",
        key="npsn_q",
    )

    if q and q.strip():
        base = q.strip().split("_")[0]
        st.session_state.last_q = q

        # O(1) lookup — never scans DataFrame
        row_positions = idx_map.get(base, [])

        if row_positions:
            hasil = data.iloc[row_positions].copy()

            # Success banner
            st.markdown(f"""
            <div class="notif">
              <div style="font-size:26px">✅</div>
              <div>
                <div class="notif-title">🎉 Pencarian Berhasil!</div>
                <div class="notif-detail">
                  NPSN <b>{base}</b> &nbsp;•&nbsp;
                  <b>{len(hasil)} instalasi</b> ditemukan &nbsp;•&nbsp;
                  {datetime.now().strftime("%H:%M:%S")}
                </div>
              </div>
              <div class="notif-badge">✓ {len(hasil)} Data</div>
            </div>""", unsafe_allow_html=True)

            hasil["_grp"] = hasil["npsn"].astype(str).str.split("_").str[0]
            for grp, grp_df in hasil.groupby("_grp"):
                sheets_info = " · ".join(grp_df["source_sheet"].astype(str).unique())
                display_df  = grp_df.drop(columns=["_grp"]).reset_index(drop=True)

                # Build & cache HTML table per (url, group, theme)
                tbl_key = (url_hash, grp, DM)
                if tbl_key not in st.session_state.html_tbl_cache:
                    st.session_state.html_tbl_cache[tbl_key] = _build_table_html(display_df, DM)
                tbl_html = st.session_state.html_tbl_cache[tbl_key]

                st.markdown(f"""
                <div class="rcard">
                  <div class="rcard-hdr">
                    <span class="rcard-title">🏫 NPSN {grp}</span>
                    <span class="rcard-badge">{len(grp_df)} instalasi</span>
                    <span class="rcard-sheet">📄 {sheets_info}</span>
                  </div>
                  {tbl_html}
                </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="nf">
              <div style="font-size:30px;margin-bottom:8px">🔍</div>
              <h4>Data Tidak Ditemukan</h4>
              <p>NPSN <b>{base}</b> tidak ada dalam database.</p>
            </div>""", unsafe_allow_html=True)

else:
    st.markdown(f"""
    <div class="nf" style="padding:36px">
      <div style="font-size:30px;margin-bottom:8px">📋</div>
      <h4>Belum Ada Data</h4>
      <p>Masukkan URL Google Spreadsheet lalu klik <b>Load / Refresh Data</b>.</p>
    </div>""", unsafe_allow_html=True)

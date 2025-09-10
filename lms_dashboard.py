# lms_dashboard_plus.py
# Streamlit: Fast leads viewer (from scratch) + enhanced features — share URL functionality removed
# Requirements:
# pip install streamlit pandas openpyxl xlsxwriter streamlit-autorefresh plotly gspread google-auth-oauthlib pymongo python-dateutil

import os
import io
import re
import json
from typing import List, Dict, Any
import certifi
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

# Google API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import gspread

# Mongo imports
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure


# -----------------------------
# Helpers
# -----------------------------
def _ensure_date(val):
    """Convert Streamlit state (date, datetime, or str) into datetime.date."""
    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str) and val:
        try:
            return pd.to_datetime(val, errors="coerce").date()
        except Exception:
            return date.today()
    return date.today()


def _get_secret(path: str, default: str = "") -> str:
    """
    Read configuration from environment variables only.
    path: dot.notation like "mongo.uri" -> checks env var MONGO_URI
    """
    env_key = path.upper().replace('.', '_')
    val = os.getenv(env_key)
    return val if val else default


def safe_head_unique_vals(series: pd.Series, max_unique=1000):
    vals = series.dropna().astype(str).replace("", pd.NA).dropna().unique()
    return sorted(vals) if len(vals) <= max_unique else []


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    df2 = df.copy()
    # Convert datetime-like series for Excel compatibility
    for c in df2.columns:
        try:
            series = pd.to_datetime(df2[c], errors="coerce", utc=False)
            if series.notna().any():
                df2[c] = series.dt.tz_localize(None)
        except Exception:
            pass
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        df2.to_excel(w, index=False, sheet_name="Leads")
    return out.getvalue()


def to_excel_bytes_multi(df: pd.DataFrame) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Leads")
        # summaries
        for col, sheet in [("State", "ByState"), ("Course", "ByCourse"), ("Source", "BySource"), ("City", "ByCity")]:
            if col in df.columns and not df[col].dropna().empty:
                vc = df[col].astype(str).replace(
                    "", pd.NA).dropna().value_counts().reset_index()
                vc.columns = [col, "Leads"]
                vc.to_excel(w, index=False, sheet_name=sheet)
        if "SortKey" in df.columns:
            trend = df.groupby(
                df["SortKey"].dt.date).size().reset_index(name="Leads")
            trend.to_excel(w, index=False, sheet_name="Trend")
    return out.getvalue()


def get_file_mtime(path: str) -> float:
    if os.path.exists(path):
        return os.path.getmtime(path)
    csv_path = os.path.splitext(path)[0] + ".csv"
    return os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0.0


# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="Leads Dashboard — Fast+", layout="wide")
DEFAULT_FILE = "Main.xlsx"
DOWNLOAD_PATH = "/Users/vineetsaraswat/Desktop/FIREBASE-DB-MAIN/Main.xlsx"
SHEET_ID = "1Jgtf8bf3qO2FXHuy8GCHZWd3nbhrLuMQrt794kJEF7Q"
SHEET_NAME = "Main"
PAGE_SIZE_DEFAULT = 100
AUTO_REFRESH_MS = 30 * 60 * 1000  # 30 minutes

EXPECTED_COLS: List[str] = [
    "Date", "Time", "Full Name", "Phone Number", "Email", "City", "State",
    "Course_City", "Course_State", "Course", "Target Country", "Intake Year",
    "Target College Name", "Target College City", "Target College State",
    "Source", "created_time", "Ad-set Name", "Ad-set ID", "Form Name", "Campaign Name",
    "Number_Course", "Mode", "Form Id", "Database Creation Date", "Database Creation Time",
    "Number_Course 2", "Spreadsheet Source"
]

# Mongo config (env var fallback)
DEFAULT_MONGO_URI = (
    "mongodb+srv://vineetsaraswat_db_user:2epv36YctAQiZwts@cluster0.zccrbma.mongodb.net/"
    "?retryWrites=true&w=majority&appName=Cluster0"
)
CONNECTION_STRING = _get_secret("mongo.uri", DEFAULT_MONGO_URI)
DATABASE_NAME = _get_secret("mongo.database", "Main")
COLLECTION_NAME = _get_secret("mongo.collection", "Main Data")


# -----------------------------
# Auto-refresh
# -----------------------------
st_autorefresh(interval=AUTO_REFRESH_MS, key="global_autorefresh")


# -----------------------------
# Google Sheets → Excel downloader
# -----------------------------
def download_sheet_to_excel(sheet_id: str, sheet_name: str, out_path: str) -> bool:
    """Download Google Sheet and save as Excel using OAuth2. Returns True on success."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("credentials.json"):
                st.error("Missing credentials.json for Google OAuth.")
                return False
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", scopes)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        client = gspread.authorize(creds)
        sh = client.open_by_key(sheet_id)
        ws = sh.worksheet(sheet_name)
        data = ws.get_all_records()
    except Exception as e:
        st.error(f"Failed to read Google Sheet: {e}")
        return False

    if not data:
        df = pd.DataFrame(columns=EXPECTED_COLS)
    else:
        df = pd.DataFrame(data)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    try:
        df.to_excel(out_path, index=False, engine="openpyxl")
    except Exception:
        df.to_csv(os.path.splitext(out_path)[0] + ".csv", index=False)
    return True


# -----------------------------
# Load data from MongoDB (cached)
# -----------------------------
# -----------------------------
# Load data from MongoDB (cached)
# -----------------------------
@st.cache_data(show_spinner="Loading leads from MongoDB…", ttl=10 * 60)
def load_data_from_mongo() -> pd.DataFrame:
    """Fetch leads directly from MongoDB and return as pandas DataFrame."""
    client = None
    try:
        client = pymongo.MongoClient(
            CONNECTION_STRING,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=20000,
            connectTimeoutMS=20000,
            socketTimeoutMS=20000,
        )
        client.admin.command("ping")  # confirm connection
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        data = list(collection.find({}, {"_id": 0}))

        if not data:
            return pd.DataFrame(columns=EXPECTED_COLS)

        df = pd.DataFrame(data)

        # Normalize columns
        df.columns = [str(c).strip() for c in df.columns]
        for c in EXPECTED_COLS:
            if c not in df.columns:
                df[c] = ""

        # Parse dates
        df["Date_parsed"] = pd.to_datetime(df["Date"].astype(str), dayfirst=True, errors="coerce")
        try:
            t = pd.to_datetime(df["Time"].astype(str), errors="coerce").dt.time
            lead_dt_str = df["Date_parsed"].dt.date.astype(str) + " " + pd.Series(t).astype(str)
            df["LeadDateTime"] = pd.to_datetime(lead_dt_str, errors="coerce")
            df.loc[df["LeadDateTime"].isna(), "LeadDateTime"] = df["Date_parsed"]
        except Exception:
            df["LeadDateTime"] = df["Date_parsed"]

        created_raw = df["created_time"].astype(str).str.replace('"', '', regex=False)
        df["created_dt"] = pd.to_datetime(created_raw, errors="coerce", dayfirst=True)
        if df["created_dt"].isna().all():
            df["created_dt"] = pd.to_datetime(created_raw, errors="coerce")

        df["SortKey"] = pd.to_datetime(
            df["LeadDateTime"].fillna(df["created_dt"]), errors="coerce"
        )

        # Phone normalization
        if "Phone Number" in df.columns:
            df["Phone Number"] = (
                df["Phone Number"].astype(str).str.replace(r"[^\d]", "", regex=True).str.strip()
            )

        df["Date_formatted"] = pd.to_datetime(
            df["Date_parsed"], errors="coerce"
        ).dt.strftime("%d/%m/%Y")

        # Duplicate detection
        df["dup_phone"] = df["Phone Number"].where(df["Phone Number"].ne(""))
        df["dup_email"] = df["Email"].where(df["Email"].astype(str).str.strip().ne(""))
        df["dup_num_course2"] = df["Number_Course 2"].where(
            df["Number_Course 2"].astype(str).str.strip().ne("")
        )

        df["is_dup_phone"] = df.duplicated(subset=["dup_phone"], keep=False) & df["dup_phone"].notna()
        df["is_dup_email"] = df.duplicated(subset=["dup_email"], keep=False) & df["dup_email"].notna()
        df["is_dup_num_course2"] = df.duplicated(
            subset=["dup_num_course2"], keep=False
        ) & df["dup_num_course2"].notna()

        df.drop(columns=["dup_phone", "dup_email", "dup_num_course2"], inplace=True)

        # Quick action links
        if "Phone Number" in df.columns:
            def _wa(num: str) -> str:
                n = str(num)
                if not n or n == "nan":
                    return ""
                if len(n) == 10:
                    n = "91" + n
                return f"https://wa.me/{n}"
            df["WhatsApp"] = df["Phone Number"].apply(_wa)
            df["Tel"] = df["Phone Number"].apply(
                lambda x: f"tel:{x}" if str(x).strip() else ""
            )

        # Reverse (newest first)
        df = df.iloc[::-1].reset_index(drop=True)
        return df

    except (ConnectionFailure, OperationFailure) as e:
        st.error(f"❌ MongoDB connection failed: {str(e)}")
        return pd.DataFrame(columns=EXPECTED_COLS)
    except Exception as e:
        st.error(f"❌ Unexpected error while fetching MongoDB data: {str(e)}")
        return pd.DataFrame(columns=EXPECTED_COLS)
    finally:
        if client:
            client.close()



# -----------------------------
# Fast-search preparation (cached)
# -----------------------------
@st.cache_data
def prepare_fast_search(df: pd.DataFrame, cols=("Full Name", "Phone Number", "Email", "City")) -> pd.DataFrame:
    tmp = df.copy()
    parts = []
    for c in cols:
        if c in tmp.columns:
            parts.append(tmp[c].astype(str).fillna("").str.lower())
        else:
            parts.append(pd.Series([""] * len(tmp)))
    tmp["__search"] = parts[0]
    for p in parts[1:]:
        tmp["__search"] = (tmp["__search"].astype(
            str) + " " + p.astype(str)).str.strip()
    for c in cols:
        if c in tmp.columns:
            tmp[f"__{c}_lc"] = tmp[c].astype(str).fillna("").str.lower()
    return tmp


def apply_filters(df: pd.DataFrame, global_q: str, col_filters: dict) -> pd.DataFrame:
    work = df
    if global_q:
        q = re.escape(global_q.strip().lower())
        work = work[work["__search"].str.contains(q, na=False, regex=True)]

    for col, val in col_filters.items():
        if val is None or (isinstance(val, list) and len(val) == 0):
            continue
        if isinstance(val, list):
            work = work[work[col].astype(str).isin([str(v) for v in val])]
        else:
            pattern = r"\b" + re.escape(str(val).lower()) + r"\b"
            col_lc = f"__{col}_lc"
            if col_lc in work.columns:
                work = work[work[col_lc].str.contains(
                    pattern, na=False, regex=True)]
            else:
                work = work[work[col].astype(str).str.lower(
                ).str.contains(pattern, na=False, regex=True)]
    return work


# -----------------------------
# Styling & header
# -----------------------------
SIDEBAR_CSS = """
<style>
body, .stApp { background-color: #111 !important; color: white !important; }
.stMetric { background: linear-gradient(135deg, #222, #333) !important; border-radius: 14px; padding: 12px; box-shadow: 0 2px 6px rgba(0,0,0,0.5); transition: all 0.3s ease-in-out; }
.stMetric:hover { box-shadow: 0 0 12px rgba(0, 255, 200, 0.5); }
.stButton>button { background: linear-gradient(135deg, #444, #666); color: white; border-radius: 10px; transition: all 0.3s; padding: 8px 16px; font-weight: 600; }
.stButton>button:hover { background: linear-gradient(135deg, #666, #888); transform: scale(1.05); }
.dataframe tbody tr:hover { background-color: rgba(0, 150, 255, 0.1) !important; }
.badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; margin-left:6px; }
.badge-dup { background:#402; color:#ff6; border:1px solid #a26; }
.badge-ok { background:#023; color:#7fe; border:1px solid #17a; }
.table-actions a { margin-right: 8px; text-decoration:none; }
</style>
"""
st.markdown(SIDEBAR_CSS, unsafe_allow_html=True)

st.markdown(
    """
    <div style="position: sticky; top: 0; z-index: 999; background: #111; padding: 12px; border-bottom: 1px solid #333;">
        <h2 style="margin:0; color:#00e5ff;">⚡ Leads Dashboard Plus</h2>
    </div>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Sidebar controls (no share URL)
# -----------------------------
with st.sidebar:
    if st.button("🔄 Sync from Google Sheet"):
        ok = download_sheet_to_excel(SHEET_ID, SHEET_NAME, DOWNLOAD_PATH)
        st.success(
            "Synced Google Sheet to Excel." if ok else "Sync failed. Check credentials and Sheet access.")

# -----------------------------
# Load data
# -----------------------------
mtime = get_file_mtime(DOWNLOAD_PATH)
df_raw = load_data_from_mongo()
df_fast = prepare_fast_search(df_raw)

# -----------------------------
# Filters
# -----------------------------
with st.sidebar:
    st.markdown("### 🔍 Filters")
    _rerun = getattr(st, "rerun", None) or getattr(
        st, "experimental_rerun", None)

    if st.button("Clear filters"):
        for key in list(st.session_state.keys()):
            if key in df_fast.columns or key in ["q", "date_from", "date_to", "page", "page_size", "quick_range"]:
                del st.session_state[key]
        st.session_state["page"] = 1
        if _rerun:
            _rerun()

    q = st.text_input("Search (name / phone / email / city)",
                      key="q", value=st.session_state.get("q", "")).strip()

    st.subheader("📅 Date range")
    quick = st.radio("Quick range", [
                     "All", "Today", "7 days", "30 days", "This month"], index=0, key="quick_range")

    if df_fast.empty or df_fast["SortKey"].dropna().empty:
        min_date = date.today() - timedelta(days=30)
        max_date = date.today()
    else:
        min_date = pd.to_datetime(df_fast["SortKey"].min()).date()
        max_date = pd.to_datetime(df_fast["SortKey"].max()).date()
        if min_date > max_date:
            min_date, max_date = max_date, min_date

    if quick == "All":
        default_from = st.session_state.get("date_from", min_date)
        default_to = st.session_state.get("date_to", max_date)
    elif quick == "Today":
        default_from = default_to = date.today()
    elif quick == "7 days":
        default_from = date.today() - timedelta(days=7)
        default_to = date.today()
    elif quick == "30 days":
        default_from = date.today() - timedelta(days=30)
        default_to = date.today()
    else:
        today_ = date.today()
        default_from = today_.replace(day=1)
        default_to = today_

    default_from = max(min_date, min(default_from, max_date))
    default_to = max(min_date, min(default_to, max_date))
    if default_from > default_to:
        default_from, default_to = min_date, max_date

    date_from = st.date_input("From", value=default_from,
                              min_value=min_date, max_value=max_date, key="date_from")
    date_to = st.date_input(
        "To", value=default_to, min_value=min_date, max_value=max_date, key="date_to")

    col_filters: Dict[str, Any] = {}
    for col in EXPECTED_COLS:
        if col not in df_fast.columns:
            continue
        options = safe_head_unique_vals(df_fast[col])
        if not options:
            continue
        selected = st.multiselect(
            col, options=["Select All"] + options, default=[], key=col)
        if "Select All" in selected:
            selected = options
        if selected:
            col_filters[col] = selected

# -----------------------------
# Apply filters to data
# -----------------------------
filtered = apply_filters(df_fast, st.session_state.get("q", ""), col_filters)

if "date_from" in st.session_state and "date_to" in st.session_state:
    start_date = pd.to_datetime(st.session_state["date_from"])
    end_date = pd.to_datetime(
        st.session_state["date_to"]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    if "SortKey" in filtered.columns:
        filtered = filtered[(filtered["SortKey"] >= start_date)
                            & (filtered["SortKey"] <= end_date)]

# -----------------------------
# KPIs
# -----------------------------
# -----------------------------
# KPIs
# -----------------------------
total_all, total_filtered = len(df_fast), len(filtered)
col1, col2, col3, col4, col5, col6 = st.columns(6)  # add 6th column
col1.metric("📊 Total leads", f"{total_all:,}")
col2.metric("🔎 In view", f"{total_filtered:,}")

latest_ts_series = (filtered["LeadDateTime"] if (not filtered.empty and "LeadDateTime" in filtered.columns)
                    else df_fast.get("LeadDateTime", pd.Series([], dtype="datetime64[ns]")))
latest_ts = pd.to_datetime(latest_ts_series).max(
) if not latest_ts_series.empty else pd.NaT
col3.metric("⏱ Latest lead", latest_ts.strftime(
    "%Y-%m-%d %H:%M:%S") if pd.notna(latest_ts) else "—")

active_campaigns = 0
if not filtered.empty and "Campaign Name" in filtered.columns:
    active_campaigns = filtered["Campaign Name"].dropna().astype(
        str).str.strip().replace("", pd.NA).dropna().nunique()
col4.metric("📢 Active campaigns", f"{active_campaigns:,}")

col5.metric("🧬 Potential duplicates (all)", int(
    filtered["is_dup_phone"].sum() +
    filtered["is_dup_email"].sum() +
    filtered["is_dup_num_course2"].sum()
    if not filtered.empty else 0
))

# NEW: count duplicates from Number_Course 2 only
dup_num_course2_count = int(
    filtered["is_dup_num_course2"].sum() if not filtered.empty else 0)
col6.metric("📚 Duplicates (Number_Course 2)", dup_num_course2_count)


st.info("💡 Tip: Use Global Search for fast lookup across name, phone, email, and city.")
st.markdown("---")

# -----------------------------
# Downloads + Preview
# -----------------------------
dl1, dl2, dl3, dl4 = st.columns(4)
dl1.download_button("⬇️ CSV (filtered)", filtered.to_csv(
    index=False).encode("utf-8"), file_name="leads_filtered.csv")
dl2.download_button("⬇️ Excel (filtered)", to_excel_bytes(
    filtered), file_name="leads_filtered.xlsx")
dl3.download_button("⬇️ CSV (all)", df_raw.to_csv(
    index=False).encode("utf-8"), file_name="leads_all.csv")
dl4.download_button("⬇️ Excel+Summaries", to_excel_bytes_multi(filtered),
                    file_name="leads_filtered_plus.xlsx")

st.markdown("---")

# -----------------------------
# Settings (Theme + Page Size + Table mode)
# -----------------------------
with st.sidebar:
    st.markdown("### ⚙️ Settings")
    theme_choice = st.radio(
        "Theme", ["Dark", "Light"], index=0, key="theme_choice")

    def apply_theme(theme: str):
        if theme == "Light":
            st.markdown("""
                <style>
                body, .stApp { background-color: #eaf4fc; color: #222; }
                .stMetric { background: white !important; border-radius: 12px; padding: 10px; box-shadow: 0 1px 4px rgba(0,0,0,0.1); }
                .stButton>button { background-color: #1e88e5; color: white; border-radius: 8px; transition: 0.3s; }
                .stButton>button:hover { background-color: #1565c0; }
                </style>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
                <style>
                body, .stApp { background-color: #111; color: white; }
                .stMetric { background: #1e1e1e !important; border-radius: 12px; padding: 10px; box-shadow: 0 1px 4px rgba(255,255,255,0.05); }
                .stButton>button { background-color: #444; color: white; border-radius: 8px; transition: 0.3s; }
                .stButton>button:hover { background-color: #666; }
                </style>
            """, unsafe_allow_html=True)

    apply_theme(theme_choice)

    page_size_options = [25, 50, 100, 250, 500]
    st.session_state["page_size"] = st.selectbox(
        "Rows per page", page_size_options, index=page_size_options.index(PAGE_SIZE_DEFAULT))

    table_mode = st.radio(
        "Table mode", ["Fast (static)", "Interactive (beta)"], index=0, key="table_mode")

# -----------------------------
# Sidebar Info
# -----------------------------
with st.sidebar:
    st.markdown("### 👤 Dashboard Info")
    file_path = DOWNLOAD_PATH
    if not os.path.exists(file_path):
        csv_path = os.path.splitext(DOWNLOAD_PATH)[0] + ".csv"
        if os.path.exists(csv_path):
            file_path = csv_path
    st.write(f"**File in use:** `{os.path.basename(file_path)}`")
    st.write(
        f"**Last reload:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.markdown("---")
    st.markdown(
        f"📎 [Google Sheet Link](https://docs.google.com/spreadsheets/d/{SHEET_ID})")

# -----------------------------
# Pagination
# -----------------------------
if "page" not in st.session_state:
    st.session_state.page = 1
PAGE_SIZE = int(st.session_state.get("page_size", PAGE_SIZE_DEFAULT))

total_rows = len(filtered)
total_pages = max(1, (total_rows + PAGE_SIZE - 1) // PAGE_SIZE)
st.session_state.page = min(max(1, st.session_state.page), total_pages)

start, end = (st.session_state.page - 1) * \
    PAGE_SIZE, (st.session_state.page) * PAGE_SIZE
page_df = filtered.iloc[start:end].copy()

# Ordered columns + extra
ordered_cols = [c for c in EXPECTED_COLS if c in page_df.columns]
extra_cols = [
    c for c in page_df.columns if c not in ordered_cols and not c.startswith("__")]

# Quality badges & actions
if any(c in page_df.columns for c in ["is_dup_phone", "is_dup_email", "is_dup_num_course2"]):
    def _badge(row) -> str:
        flags = []
        if row.get("is_dup_phone"):
            flags.append("phone")
        if row.get("is_dup_email"):
            flags.append("email")
        if row.get("is_dup_num_course2"):
            flags.append("num_course2")
        if not flags:
            return "<span class='badge badge-ok'>unique</span>"
        return f"<span class='badge badge-dup'>dup: {','.join(flags)}</span>"
    page_df["Quality"] = page_df.apply(lambda r: _badge(r.to_dict()), axis=1)

if "WhatsApp" in page_df.columns or "Tel" in page_df.columns or "Email" in page_df.columns:
    def _actions(row) -> str:
        parts = ["<span class='table-actions'>"]
        if row.get("Tel"):
            parts.append(
                f"<a href='{row.get('Tel')}' target='_blank'>☎️ Call</a>")
        if row.get("WhatsApp"):
            parts.append(
                f"<a href='{row.get('WhatsApp')}' target='_blank'>💬 WhatsApp</a>")
        if row.get("Email"):
            parts.append(
                f"<a href='mailto:{row.get('Email')}' target='_blank'>✉️ Email</a>")
        parts.append("</span>")
        return " ".join(parts)
    page_df["Actions"] = page_df.apply(lambda r: _actions(r), axis=1)

page_df_display = page_df.loc[:, ordered_cols + extra_cols +
                              [c for c in ["Quality", "Actions"] if c in page_df.columns]].copy()
if "Date_formatted" in page_df_display.columns and "Date" in page_df_display.columns:
    page_df_display.loc[:, "Date"] = page_df_display["Date_formatted"]

nav1, nav2, nav3, nav4 = st.columns([1, 1, 1, 4])
if nav1.button("⏮ First", disabled=(st.session_state.page == 1)):
    st.session_state.page = 1
    if _rerun:
        _rerun()
if nav2.button("◀ Prev", disabled=(st.session_state.page == 1)):
    st.session_state.page -= 1
    if _rerun:
        _rerun()
if nav3.button("Next ▶", disabled=(st.session_state.page == total_pages)):
    st.session_state.page += 1
    if _rerun:
        _rerun()
if nav4.button("⏭ Last", disabled=(st.session_state.page == total_pages)):
    st.session_state.page = total_pages
    if _rerun:
        _rerun()

st.write(
    f"📄 Page {st.session_state.page}/{total_pages} — Showing {start+1:,}–{min(end,total_rows):,} of {total_rows:,}")

# Toggle between fast and interactive table
if st.session_state.get("table_mode") == "Interactive (beta)":
    try:
        editable_cols = {c: False for c in page_df_display.columns}
        sel = st.data_editor(
            page_df_display,
            use_container_width=True,
            hide_index=True,
            disabled=editable_cols,
            column_config={"Quality": st.column_config.TextColumn(
                help="Duplicate indicator", width="small")},
            num_rows="fixed",
            key=f"editor_page_{st.session_state.page}",
        )
        st.download_button("⬇️ Download this page (CSV)", sel.to_csv(index=False).encode(
            "utf-8"), file_name=f"leads_page_{st.session_state.page}.csv")
    except Exception:
        st.dataframe(page_df_display,
                     use_container_width=True, hide_index=True)
else:
    st.dataframe(page_df_display, use_container_width=True, hide_index=True)

st.markdown("---")

# -----------------------------
# Summaries + Charts
# -----------------------------
s1, s2, s3 = st.columns(3)
with s1:
    st.write("#### Leads by State (Top 10)")
    if "State" in filtered.columns:
        s = filtered["State"].astype(str).replace("", pd.NA).dropna()
        if not s.empty:
            df_summary = s.value_counts().head(10).reset_index()
            df_summary.columns = ["State", "Leads"]
            st.dataframe(df_summary, hide_index=True)
            st.plotly_chart(px.pie(df_summary, names="State", values="Leads",
                            title="State Distribution"), use_container_width=True)
        else:
            st.caption("No data.")

with s2:
    st.write("#### Leads by Course (Top 10)")
    if "Course" in filtered.columns:
        s = filtered["Course"].astype(str).replace("", pd.NA).dropna()
        if not s.empty:
            df_summary = s.value_counts().head(10).reset_index()
            df_summary.columns = ["Course", "Leads"]
            st.dataframe(df_summary, hide_index=True)
            st.plotly_chart(px.bar(df_summary, x="Course", y="Leads",
                            title="Top Courses"), use_container_width=True)
        else:
            st.caption("No data.")

with s3:
    st.write("#### Leads by Source (Top 10)")
    if "Source" in filtered.columns:
        s = filtered["Source"].astype(str).replace("", pd.NA).dropna()
        if not s.empty:
            df_summary = s.value_counts().head(10).reset_index()
            df_summary.columns = ["Source", "Leads"]
            st.dataframe(df_summary, hide_index=True)
            st.plotly_chart(px.bar(df_summary, x="Source", y="Leads",
                            title="Top Sources"), use_container_width=True)
        else:
            st.caption("No data.")

# Top Cities
if "City" in filtered.columns:
    top_cities = filtered["City"].astype(str).replace("", pd.NA).dropna()
    if not top_cities.empty:
        df_cities = top_cities.value_counts().head(10).reset_index()
        df_cities.columns = ["City", "Leads"]
        st.plotly_chart(px.bar(df_cities, x="City", y="Leads",
                        title="Top 10 Cities"), use_container_width=True)

# Leads Over Time Trend
if "SortKey" in filtered.columns:
    df_trend = filtered.groupby(
        filtered["SortKey"].dt.date).size().reset_index(name="Leads")
    fig_trend = px.line(df_trend, x="SortKey", y="Leads",
                        title="Leads Over Time")
    st.plotly_chart(fig_trend, use_container_width=True)

# Hour-of-day & DOW insights
if "SortKey" in filtered.columns and not filtered["SortKey"].dropna().empty:
    time_df = filtered.dropna(subset=["SortKey"]).copy()
    time_df["Hour"] = time_df["SortKey"].dt.hour
    time_df["Weekday"] = time_df["SortKey"].dt.day_name()
    st.plotly_chart(px.bar(time_df.groupby("Hour").size().reset_index(
        name="Leads"), x="Hour", y="Leads", title="Leads by Hour"), use_container_width=True)
    st.plotly_chart(px.bar(time_df.groupby("Weekday").size().reset_index(
        name="Leads"), x="Weekday", y="Leads", title="Leads by Weekday"), use_container_width=True)

# Intake Year distribution
if "Intake Year" in filtered.columns:
    try:
        iy = filtered["Intake Year"].astype(
            str).str.extract(r"(\d{4})")[0].dropna()
        if not iy.empty:
            dist = iy.value_counts().sort_index().reset_index()
            dist.columns = ["Year", "Leads"]
            st.plotly_chart(px.bar(dist, x="Year", y="Leads",
                            title="Intake Year Distribution"), use_container_width=True)
    except Exception:
        pass

st.markdown("---")

# -----------------------------
# Export All Filters (improved)
# -----------------------------
with st.sidebar:
    st.markdown("### ⬇️ Export Filtered Leads")
    if st.button("Prepare export (CSV, Excel, JSON)"):
        csv_bytes = filtered.to_csv(index=False).encode("utf-8")
        excel_bytes = to_excel_bytes(filtered)
        json_bytes = filtered.to_json(orient="records").encode("utf-8")
        st.download_button("CSV", csv_bytes, "leads_filtered_all.csv")
        st.download_button("Excel", excel_bytes, "leads_filtered_all.xlsx")
        st.download_button("JSON", json_bytes, "leads_filtered_all.json")

# -----------------------------
# Data Quality Report
# -----------------------------
with st.expander("🧪 Data Quality Report", expanded=False):
    issues = {}
    if not filtered.empty:
        issues["Missing Phone"] = int(filtered["Phone Number"].astype(
            str).str.strip().eq("").sum()) if "Phone Number" in filtered.columns else 0
        issues["Missing Email"] = int(filtered["Email"].astype(
            str).str.strip().eq("").sum()) if "Email" in filtered.columns else 0
        issues["Duplicate Phones"] = int(
            filtered["is_dup_phone"].sum()) if "is_dup_phone" in filtered.columns else 0
        issues["Duplicate Emails"] = int(
            filtered["is_dup_email"].sum()) if "is_dup_email" in filtered.columns else 0
        dq_df = pd.DataFrame([issues]).T.reset_index()
        dq_df.columns = ["Issue", "Count"]
        st.dataframe(dq_df, hide_index=True, use_container_width=True)
    else:
        st.caption("No data in current view.")

# -----------------------------
# Cleanup / final touches
# -----------------------------
if "Phone Number" in df_fast.columns:
    df_fast["Phone Number"] = df_fast["Phone Number"].astype(
        str).str.replace(r"[^\d]", "", regex=True)
if "Intake Year" in df_fast.columns:
    df_fast["Intake Year"] = df_fast["Intake Year"].astype(
        str).str.replace(r"[^\d]", "", regex=True)

from urllib.parse import urlencode, parse_qs, urlparse

# -----------------------------
# Handle Query Params (Load filters from URL)
# -----------------------------
query_params = st.query_params

# Restore session state from query params
for key, val in query_params.items():
    if key not in st.session_state:
        if isinstance(val, list) and len(val) == 1:
            val = val[0]
        st.session_state[key] = val

# -----------------------------
# Sidebar: Add Share URL Button
# -----------------------------
with st.sidebar:
    if st.button("🔗 Share URL"):
        # Collect active filters from session state
        params = {}
        for k, v in st.session_state.items():
            if v and k in ["q", "date_from", "date_to", "quick_range"] + EXPECTED_COLS:
                params[k] = v

        # Build full URL with filters
        base_url = st.get_option("server.baseUrlPath") or ""
        current_url = st.request.url if hasattr(st, "request") else ""
        if not current_url:
            current_url = "http://localhost:8501"
        share_url = current_url.split("?")[0] + "?" + urlencode(params, doseq=True)

        # Show text box + copy button (auto copy with JS)
        st.text_input("Sharable URL", share_url, key="share_url_box")
        st.markdown(
            f"""
            <script>
            navigator.clipboard.writeText("{share_url}");
            </script>
            """,
            unsafe_allow_html=True,
        )
        st.success("✅ URL copied to clipboard! Share it with others.")

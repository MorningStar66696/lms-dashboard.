# analytics_plus.py
# Streamlit: Analytics Dashboard (extended with Leads Dashboard Plus features)
# Requirements:
# pip install streamlit pandas openpyxl xlsxwriter streamlit-autorefresh plotly pymongo python-dateutil gspread google-auth-oauthlib

import os
import io
import re
import json
import base64
from typing import Dict, Any
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure

# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="📈 Analytics Dashboard", layout="wide")
AUTO_REFRESH_MS = 30 * 60 * 1000  # 30 min
PAGE_SIZE_DEFAULT = 100

EXPECTED_COLS = [
    "Date", "Time", "Full Name", "Phone Number", "Email", "City", "State",
    "Course", "Intake Year", "Target College Name", "Target College City",
    "Source", "Campaign Name", "Ad-set Name", "Ad-set ID", "Mode",
    "Number_Course", "Number_Course 2", "created_time"
]

# -----------------------------
# Auto-refresh
# -----------------------------
st_autorefresh(interval=AUTO_REFRESH_MS, key="analytics_autorefresh")

# -----------------------------
# MongoDB connection
# -----------------------------
DEFAULT_MONGO_URI = (
    "mongodb+srv://vineetsaraswat_db_user:2epv36YctAQiZwts@cluster0.zccrbma.mongodb.net/"
    "?retryWrites=true&w=majority&appName=Cluster0"
)
CONNECTION_STRING = os.getenv("MONGO_URI", DEFAULT_MONGO_URI)
DATABASE_NAME = os.getenv("MONGO_DB", "Main")
COLLECTION_NAME = os.getenv("MONGO_COL", "Main Data")

# -----------------------------
# Load Data
# -----------------------------
@st.cache_data(show_spinner="Loading leads from MongoDB…", ttl=600)
def load_data() -> pd.DataFrame:
    try:
        client = pymongo.MongoClient(CONNECTION_STRING)
        client.admin.command("ping")
        coll = client[DATABASE_NAME][COLLECTION_NAME]
        data = list(coll.find({}, {"_id": 0}))
        client.close()

        if not data:
            return pd.DataFrame(columns=EXPECTED_COLS)

        df = pd.DataFrame(data)
        df.columns = [str(c).strip() for c in df.columns]

        # Ensure expected columns exist
        for c in EXPECTED_COLS:
            if c not in df.columns:
                df[c] = ""

        # Parse dates
        df["Date_parsed"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
        df["created_dt"] = pd.to_datetime(df["created_time"], errors="coerce")
        df["SortKey"] = pd.to_datetime(
            df["Date_parsed"].fillna(df["created_dt"]), errors="coerce"
        )

        # Clean phone numbers
        if "Phone Number" in df.columns:
            df["Phone Number"] = (
                df["Phone Number"].astype(str).str.replace(r"[^\d]", "", regex=True).str.strip()
            )

        # Reverse: newest first
        df = df.iloc[::-1].reset_index(drop=True)
        return df

    except (ConnectionFailure, OperationFailure) as e:
        st.error(f"❌ MongoDB error: {e}")
        return pd.DataFrame(columns=EXPECTED_COLS)

df_raw = load_data()

# -----------------------------
# Prepare Search Column
# -----------------------------
def prepare_fast_search(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    parts = []
    for c in ["Full Name", "Phone Number", "Email", "City"]:
        if c in tmp.columns:
            parts.append(tmp[c].astype(str).fillna("").str.lower())
        else:
            parts.append(pd.Series([""] * len(tmp)))
    tmp["__search"] = parts[0]
    for p in parts[1:]:
        tmp["__search"] = (tmp["__search"].astype(str) + " " + p.astype(str)).str.strip()
    return tmp

df_fast = prepare_fast_search(df_raw)

# -----------------------------
# Filters
# -----------------------------
with st.sidebar:
    with st.expander("🔍 Filters", expanded=True):
        q = st.text_input(
            "Search (name / phone / email / city)", key="q", value=st.session_state.get("q", "")
        ).strip()

        st.subheader("📅 Date range")
        if df_fast.empty or df_fast["SortKey"].dropna().empty:
            min_date, max_date = date.today() - timedelta(days=30), date.today()
        else:
            min_date = pd.to_datetime(df_fast["SortKey"].min()).date()
            max_date = pd.to_datetime(df_fast["SortKey"].max()).date()

        date_from = st.date_input(
            "From", value=min_date, min_value=min_date, max_value=max_date, key="date_from"
        )
        date_to = st.date_input(
            "To", value=max_date, min_value=min_date, max_value=max_date, key="date_to"
        )

        col_filters: Dict[str, Any] = {}
        for col in EXPECTED_COLS:
            if col not in df_fast.columns:
                continue
            options = sorted(df_fast[col].dropna().astype(str).unique())
            if not options:
                continue
            selected = st.multiselect(col, ["Select All"] + options, key=col)
            if "Select All" in selected:
                selected = options
            if selected:
                col_filters[col] = selected

# -----------------------------
# Apply Filters
# -----------------------------
def apply_filters(df: pd.DataFrame, global_q: str, col_filters: dict) -> pd.DataFrame:
    work = df
    if global_q:
        q = re.escape(global_q.lower())
        work = work[work["__search"].str.contains(q, na=False, regex=True)]

    for col, sel in col_filters.items():
        work = work[work[col].astype(str).isin(sel)]
    return work

filtered = apply_filters(df_fast, q, col_filters)

if "date_from" in st.session_state and "date_to" in st.session_state:
    start_date = pd.to_datetime(st.session_state["date_from"])
    end_date = pd.to_datetime(st.session_state["date_to"])
    filtered = filtered[
        (filtered["SortKey"].dt.date >= start_date.date()) &
        (filtered["SortKey"].dt.date <= end_date.date())
    ]

# -----------------------------
# Analytics
# -----------------------------
st.title("📊 Trends & Analytics")

# Leads over time
if not filtered.empty and "SortKey" in filtered.columns:
    df_time = filtered.dropna(subset=["SortKey"])
    df_time = df_time.groupby(df_time["SortKey"].dt.date).size().reset_index(name="Leads")
    st.plotly_chart(px.line(df_time, x="SortKey", y="Leads", title="Leads Over Time"), use_container_width=True)

# Leads by Intake Year
if "Intake Year" in filtered.columns:
    s = filtered["Intake Year"].astype(str).replace("", pd.NA).dropna()
    if not s.empty:
        df_summary = s.value_counts().reset_index()
        df_summary.columns = ["Intake Year", "Leads"]
        st.plotly_chart(px.bar(df_summary, x="Intake Year", y="Leads", title="Leads by Intake Year"), use_container_width=True)

# Leads by State
if "State" in filtered.columns:
    s = filtered["State"].astype(str).replace("", pd.NA).dropna()
    if not s.empty:
        df_summary = s.value_counts().head(10).reset_index()
        df_summary.columns = ["State", "Leads"]
        st.plotly_chart(px.pie(df_summary, names="State", values="Leads", title="Top 10 States"), use_container_width=True)

# Leads by Course
if "Course" in filtered.columns:
    s = filtered["Course"].astype(str).replace("", pd.NA).dropna()
    if not s.empty:
        df_summary = s.value_counts().head(10).reset_index()
        df_summary.columns = ["Course", "Leads"]
        st.plotly_chart(px.bar(df_summary, x="Course", y="Leads", title="Top 10 Courses"), use_container_width=True)

# Leads by Source
if "Source" in filtered.columns:
    s = filtered["Source"].astype(str).replace("", pd.NA).dropna()
    if not s.empty:
        df_summary = s.value_counts().head(10).reset_index()
        df_summary.columns = ["Source", "Leads"]
        st.plotly_chart(px.bar(df_summary, x="Source", y="Leads", title="Top 10 Sources"), use_container_width=True)

st.markdown("---")
st.info("💡 More analytics & insights coming soon (conversion %, top campaigns, client-wise distribution, etc.)")

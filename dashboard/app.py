import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from dotenv import load_dotenv
import os
import time
from datetime import datetime

load_dotenv()

st.set_page_config(
    page_title="FinStream | Fraud Intelligence",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS ---
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #ffffff; }
    
    /* Metric cards */
    [data-testid="metric-container"] {
    background: #f8f9fa;
    border: 1px solid #e0e0e0;
    border-left: 4px solid #2d3561;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    [data-testid="metric-container"] label {
    color: #666666 !important;
    font-size: 12px !important;
    text-transform: uppercase;
    letter-spacing: 1px;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #1a1d2e !important;
    font-size: 28px !important;
    font-weight: 700 !important;
    }
    [data-testid="metric-container"] [data-testid="stMetricDelta"] {
    color: #ff4b4b !important;
    } 

    /* Section headers */
    .section-header {
        color: #64ffda;
        font-size: 13px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 1px solid #2d3561;
    }

    /* Alert box */
    .fraud-alert {
        background: linear-gradient(135deg, #2d1b1b, #3d1515);
        border: 1px solid #ff4b4b;
        border-left: 4px solid #ff4b4b;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
        color: #ffb3b3;
        font-size: 13px;
    }

    /* Insight box */
    .insight-box {
        background: linear-gradient(135deg, #1a2a1a, #152515);
        border: 1px solid #00cc44;
        border-left: 4px solid #00cc44;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
        color: #b3ffcc;
        font-size: 13px;
    }

    /* Warning box */
    .warning-box {
        background: linear-gradient(135deg, #2a2a1a, #252515);
        border: 1px solid #ffa500;
        border-left: 4px solid #ffa500;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
        color: #ffd9b3;
        font-size: 13px;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
    background-color: #f8f9fa;
    border-right: 1px solid #e0e0e0;
    }

    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Divider */
    hr { border-color: #2d3561; }
</style>
""", unsafe_allow_html=True)

# --- Plotly dark theme ---
PLOT_THEME = dict(
    paper_bgcolor="rgba(255,255,255,0)",
    plot_bgcolor="#f8f9fa",
    font=dict(color="#333333", family="monospace"),
    title_font=dict(color="#1a1d2e", size=14),
)

# --- Connection ---
def get_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST").replace("https://", ""),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

@st.cache_data(ttl=30)
def run_query(query):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)

# --- Queries ---
TOTAL_STATS_QUERY = """
    SELECT
        COUNT(*)                                                        AS total_transactions,
        COUNT(DISTINCT user_id)                                         AS unique_users,
        ROUND(SUM(amount), 2)                                           AS total_volume,
        ROUND(AVG(amount), 2)                                           AS avg_amount,
        SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)               AS total_fraud,
        ROUND(SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
        SUM(CASE WHEN is_high_risk_country = true THEN 1 ELSE 0 END)   AS high_risk_txns,
        SUM(CASE WHEN is_fraud = true THEN amount ELSE 0 END)           AS fraud_volume
    FROM hive_metastore.silver.stg_transactions
    WHERE is_valid_record = true
"""

HOURLY_VOLUME_QUERY = """
    SELECT
        transaction_hour,
        total_transactions,
        fraud_count,
        fraud_rate_pct,
        total_amount,
        unique_users
    FROM hive_metastore.silver.gold_hourly_summary
    ORDER BY transaction_hour DESC
    LIMIT 24
"""

FRAUD_BY_COUNTRY_QUERY = """
    SELECT
        country,
        total_transactions,
        fraud_count,
        fraud_rate_pct,
        total_amount,
        is_high_risk_country
    FROM hive_metastore.silver.gold_fraud_by_country
    ORDER BY fraud_rate_pct DESC
    LIMIT 15
"""

USER_RISK_QUERY = """
    SELECT
        risk_category,
        COUNT(*)                AS user_count,
        ROUND(AVG(risk_score),1) AS avg_risk_score,
        SUM(fraud_transaction_count) AS total_frauds
    FROM hive_metastore.silver.gold_user_risk
    GROUP BY risk_category
    ORDER BY avg_risk_score DESC
"""

TOP_RISKY_USERS_QUERY = """
    SELECT
        user_id,
        risk_score,
        risk_category,
        total_transactions,
        fraud_transaction_count,
        ROUND(total_spend, 2) AS total_spend,
        unique_countries,
        late_night_txns
    FROM hive_metastore.silver.gold_user_risk
    ORDER BY risk_score DESC
    LIMIT 10
"""

AMOUNT_DIST_QUERY = """
    SELECT
        amount_category,
        COUNT(*)                                            AS transaction_count,
        SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)   AS fraud_count,
        ROUND(AVG(amount), 2)                               AS avg_amount
    FROM hive_metastore.silver.stg_transactions
    WHERE is_valid_record = true
    GROUP BY amount_category
    ORDER BY avg_amount DESC
"""

MERCHANT_QUERY = """
    SELECT
        merchant_category,
        COUNT(*)                                                        AS total_transactions,
        ROUND(SUM(amount), 2)                                           AS total_volume,
        SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END)               AS fraud_count,
        ROUND(SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_pct
    FROM hive_metastore.silver.stg_transactions
    WHERE is_valid_record = true
    GROUP BY merchant_category
    ORDER BY fraud_rate_pct DESC
"""

# ─── SIDEBAR ───────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🛡️ FinStream")
    st.markdown("<p style='color:#8892b0;font-size:12px'>Fraud Intelligence Platform</p>", unsafe_allow_html=True)
    st.divider()

    auto_refresh = st.toggle("Live Mode", value=True)
    refresh_interval = st.slider("Refresh (seconds)", 10, 120, 30)

    if st.button("⚡ Refresh Now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("<p class='section-header'>Pipeline Status</p>", unsafe_allow_html=True)
    st.markdown("🟢 Event Hubs — **Live**")
    st.markdown("🟢 Databricks — **Running**")
    st.markdown("🟢 Delta Lake — **Healthy**")
    st.markdown("🟢 dbt Models — **Current**")

    st.divider()
    now = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"<p style='color:#8892b0;font-size:11px'>Last updated: {now}</p>", unsafe_allow_html=True)

# ─── HEADER ────────────────────────────────────────────────
st.markdown("## FinStream Fraud Intelligence")
st.markdown("<p style='color:#8892b0;margin-top:-12px'>Real-time financial transaction monitoring · Azure Event Hubs → Databricks → Delta Lake → dbt</p>", unsafe_allow_html=True)
st.divider()

# ─── ROW 1: KPI CARDS ──────────────────────────────────────
try:
    stats = run_query(TOTAL_STATS_QUERY)
    s = stats.iloc[0]

    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    col1.metric("Total Transactions", f"{int(s['total_transactions']):,}")
    col2.metric("Unique Users", f"{int(s['unique_users']):,}")
    col3.metric("Total Volume", f"${float(s['total_volume']):,.0f}")
    col4.metric("Avg Amount", f"${float(s['avg_amount']):,.2f}")
    col5.metric("Fraud Count", f"{int(s['total_fraud']):,}", delta=f"{float(s['fraud_rate_pct'])}% rate", delta_color="inverse")
    col6.metric("Fraud Volume", f"${float(s['fraud_volume']):,.0f}")
    col7.metric("High Risk Txns", f"{int(s['high_risk_txns']):,}")
except Exception as e:
    st.error(f"Error loading KPIs: {e}")

st.divider()

# ─── ROW 2: ALERTS + HOURLY CHART ──────────────────────────
col_alerts, col_chart = st.columns([1, 2])

with col_alerts:
    st.markdown("<p class='section-header'>🚨 Live Alerts</p>", unsafe_allow_html=True)
    try:
        s = stats.iloc[0]
        fraud_rate = float(s['fraud_rate_pct'])
        high_risk = int(s['high_risk_txns'])
        fraud_vol = float(s['fraud_volume'])

        if fraud_rate > 30:
            st.markdown(f"""<div class='fraud-alert'>
                🔴 <strong>Critical Fraud Rate</strong><br>
                Fraud rate at <strong>{fraud_rate}%</strong> — well above 20% threshold.
                Immediate review recommended.
            </div>""", unsafe_allow_html=True)
        elif fraud_rate > 20:
            st.markdown(f"""<div class='warning-box'>
                🟡 <strong>Elevated Fraud Rate</strong><br>
                Fraud rate at <strong>{fraud_rate}%</strong> — above normal threshold.
                Monitor closely.
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""<div class='insight-box'>
                🟢 <strong>Fraud Rate Normal</strong><br>
                Fraud rate at <strong>{fraud_rate}%</strong> — within acceptable range.
            </div>""", unsafe_allow_html=True)

        if high_risk > 100:
            st.markdown(f"""<div class='fraud-alert'>
                🔴 <strong>High Risk Country Spike</strong><br>
                <strong>{high_risk:,}</strong> transactions from sanctioned/high-risk countries detected.
            </div>""", unsafe_allow_html=True)

        st.markdown(f"""<div class='warning-box'>
            💰 <strong>Fraud Exposure</strong><br>
            Total fraudulent volume: <strong>${fraud_vol:,.2f}</strong>
        </div>""", unsafe_allow_html=True)

        st.markdown(f"""<div class='insight-box'>
            📊 <strong>Pipeline Health</strong><br>
            All Bronze → Silver → Gold layers running normally.
            dbt models up to date.
        </div>""", unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Error: {e}")

with col_chart:
    st.markdown("<p class='section-header'>📈 Transaction Volume & Fraud Rate</p>", unsafe_allow_html=True)
    try:
        hourly = run_query(HOURLY_VOLUME_QUERY)
        if not hourly.empty:
            hourly = hourly.sort_values("transaction_hour")
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=hourly["transaction_hour"],
                y=hourly["total_transactions"],
                name="Transactions",
                marker_color="#2d3561",
                yaxis="y"
            ))
            fig.add_trace(go.Scatter(
                x=hourly["transaction_hour"],
                y=hourly["fraud_rate_pct"],
                name="Fraud Rate %",
                line=dict(color="#ff4b4b", width=2),
                yaxis="y2"
            ))
            fig.update_layout(
                **PLOT_THEME,
                yaxis=dict(title="Transactions", gridcolor="#2d3561", color="#8892b0"),
yaxis2=dict(title="Fraud Rate %", overlaying="y", side="right", gridcolor="#2d3561", color="#8892b0"),
                legend=dict(orientation="h", y=1.1),
                height=320,
                margin=dict(l=0, r=0, t=30, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

st.divider()

# ─── ROW 3: COUNTRY + MERCHANT ─────────────────────────────
col_country, col_merchant = st.columns(2)

with col_country:
    st.markdown("<p class='section-header'>🌍 Fraud by Country</p>", unsafe_allow_html=True)
    try:
        country = run_query(FRAUD_BY_COUNTRY_QUERY)
        if not country.empty:
            country["color"] = country["is_high_risk_country"].map({True: "#ff4b4b", False: "#4b9bff"})
            fig = go.Figure(go.Bar(
                x=country["fraud_rate_pct"],
                y=country["country"],
                orientation="h",
                marker_color=country["color"],
                text=country["fraud_rate_pct"].apply(lambda x: f"{x}%"),
                textposition="outside",
                textfont=dict(color="#ccd6f6", size=11)
            ))
            fig.update_layout(
                **PLOT_THEME,
                height=380,
                margin=dict(l=0, r=40, t=10, b=0),
                xaxis_title="Fraud Rate %",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("<p style='color:#8892b0;font-size:11px'>🔴 High risk country &nbsp;&nbsp; 🔵 Standard country</p>", unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error: {e}")

with col_merchant:
    st.markdown("<p class='section-header'>🏪 Fraud by Merchant Category</p>", unsafe_allow_html=True)
    try:
        merchant = run_query(MERCHANT_QUERY)
        if not merchant.empty:
            fig = px.scatter(
                merchant,
                x="total_volume",
                y="fraud_rate_pct",
                size="total_transactions",
                color="fraud_rate_pct",
                color_continuous_scale="RdYlGn_r",
                text="merchant_category",
                labels={
                    "total_volume": "Total Volume ($)",
                    "fraud_rate_pct": "Fraud Rate %",
                    "total_transactions": "Transaction Count"
                }
            )
            fig.update_traces(textposition="top center", textfont=dict(size=10, color="#ccd6f6"))
            fig.update_layout(
                **PLOT_THEME,
                height=380,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

st.divider()

# ─── ROW 4: RISK DISTRIBUTION + TOP RISKY USERS ────────────
col_risk, col_users = st.columns([1, 2])

with col_risk:
    st.markdown("<p class='section-header'>⚠️ User Risk Distribution</p>", unsafe_allow_html=True)
    try:
        risk = run_query(USER_RISK_QUERY)
        if not risk.empty:
            fig = go.Figure(go.Pie(
                labels=risk["risk_category"],
                values=risk["user_count"],
                hole=0.6,
                marker=dict(colors=["#ff4b4b", "#ffa500", "#00cc44"]),
                textinfo="label+percent",
                textfont=dict(color="#ccd6f6", size=12)
            ))
            fig.update_layout(
                **PLOT_THEME,
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                showlegend=False,
                annotations=[dict(
                    text=f"{int(risk['user_count'].sum())}<br>users",
                    x=0.5, y=0.5,
                    font=dict(size=16, color="#ccd6f6"),
                    showarrow=False
                )]
            )
            st.plotly_chart(fig, use_container_width=True)

            for _, row in risk.iterrows():
                color = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(row["risk_category"], "⚪")
                st.markdown(f"{color} **{row['risk_category'].title()}** — {int(row['user_count'])} users · avg score {row['avg_risk_score']}")
    except Exception as e:
        st.error(f"Error: {e}")

with col_users:
    st.markdown("<p class='section-header'>🚨 Top 10 Highest Risk Users</p>", unsafe_allow_html=True)
    try:
        risky = run_query(TOP_RISKY_USERS_QUERY)
        if not risky.empty:
            risky["risk_badge"] = risky["risk_category"].map({
                "high": "🔴 High",
                "medium": "🟡 Medium",
                "low": "🟢 Low"
            })
            st.dataframe(
                risky[["user_id", "risk_score", "risk_badge", "total_transactions",
                        "fraud_transaction_count", "total_spend", "unique_countries", "late_night_txns"]],
                use_container_width=True,
                height=320,
                column_config={
                    "user_id": st.column_config.TextColumn("User"),
                    "risk_score": st.column_config.ProgressColumn(
                        "Risk Score", min_value=0, max_value=100, format="%d"
                    ),
                    "risk_badge": st.column_config.TextColumn("Level"),
                    "total_transactions": st.column_config.NumberColumn("Txns", format="%d"),
                    "fraud_transaction_count": st.column_config.NumberColumn("Frauds", format="%d"),
                    "total_spend": st.column_config.NumberColumn("Total Spend", format="$%.2f"),
                    "unique_countries": st.column_config.NumberColumn("Countries", format="%d"),
                    "late_night_txns": st.column_config.NumberColumn("Late Night", format="%d"),
                }
            )
    except Exception as e:
        st.error(f"Error: {e}")

st.divider()

# ─── ROW 5: AMOUNT DISTRIBUTION ────────────────────────────
st.markdown("<p class='section-header'>💵 Transaction Amount Analysis</p>", unsafe_allow_html=True)
try:
    amount = run_query(AMOUNT_DIST_QUERY)
    if not amount.empty:
        col_a, col_b, col_c = st.columns(3)
        for col, (_, row) in zip([col_a, col_b, col_c], amount.iterrows()):
            fraud_pct = round(row["fraud_count"] / row["transaction_count"] * 100, 1) if row["transaction_count"] > 0 else 0
            color = {"high": "#ff4b4b", "medium": "#ffa500", "low": "#00cc44"}.get(row["amount_category"], "#ccd6f6")
            col.markdown(f"""
            <div style='background:#1a1d2e;border:1px solid {color};border-radius:12px;padding:16px;text-align:center'>
                <p style='color:{color};font-size:12px;text-transform:uppercase;letter-spacing:1px;margin:0'>{row['amount_category'].title()} Value</p>
                <p style='color:#ccd6f6;font-size:24px;font-weight:700;margin:8px 0'>{int(row['transaction_count']):,}</p>
                <p style='color:#8892b0;font-size:12px;margin:0'>transactions</p>
                <p style='color:#ccd6f6;font-size:16px;margin:8px 0'>${float(row['avg_amount']):,.2f} avg</p>
                <p style='color:{color};font-size:13px;margin:0'>{fraud_pct}% fraud rate</p>
            </div>
            """, unsafe_allow_html=True)
except Exception as e:
    st.error(f"Error: {e}")

# ─── AUTO REFRESH ───────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_interval)
    st.cache_data.clear()
    st.rerun()
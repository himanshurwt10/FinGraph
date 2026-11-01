
import os
from datetime import date, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx

from dotenv import load_dotenv
from neo4j import GraphDatabase


# =====================================================
# 1. CONFIGURATION
# =====================================================

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

st.set_page_config(
    page_title="FinGraph | Fraud Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

if not all([NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD]):
    st.error("Neo4j configuration missing. Check your .env file.")
    st.stop()


# =====================================================
# 2. PROFESSIONAL DARK THEME
# =====================================================

st.markdown("""
<style>
    .stApp {
        background: #0b1120;
        color: #e5e7eb;
    }

    [data-testid="stSidebar"] {
        background: #111827;
        border-right: 1px solid #263244;
    }

    [data-testid="stMetric"] {
        background: #151f31;
        border: 1px solid #29364b;
        padding: 18px;
        border-radius: 12px;
    }

    [data-testid="stMetricLabel"] {
        color: #9ca3af;
    }

    [data-testid="stMetricValue"] {
        color: #f9fafb;
    }

    .main-title {
        font-size: 34px;
        font-weight: 750;
        color: #f9fafb;
        margin-bottom: 0;
    }

    .subtitle {
        color: #9ca3af;
        font-size: 14px;
        margin-top: 4px;
    }

    .section-title {
        font-size: 21px;
        font-weight: 650;
        color: #f3f4f6;
        margin-top: 12px;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #29364b;
        border-radius: 10px;
    }

    .status-live {
        color: #34d399;
        font-weight: 700;
    }

    div.stButton > button {
        border-radius: 8px;
        border: 1px solid #334155;
    }
</style>
""", unsafe_allow_html=True)


# =====================================================
# 3. NEO4J CONNECTION
# =====================================================

@st.cache_resource
def get_driver():
    return GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )


def run_query(query, parameters=None):
    with get_driver().session(
        database=NEO4J_DATABASE
    ) as session:
        result = session.run(
            query,
            parameters or {}
        )
        return [record.data() for record in result]


# =====================================================
# 4. HELPER FUNCTIONS
# =====================================================

def make_dataframe(data):
    return pd.DataFrame(data) if data else pd.DataFrame()


def format_currency(value):
    return f"₹{value:,.2f}"


def apply_plotly_theme(fig, height=350):
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#151f31",
        plot_bgcolor="#151f31",
        font=dict(color="#e5e7eb"),
        height=height,
        margin=dict(l=20, r=20, t=45, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    return fig


# =====================================================
# 5. FRAUD ALERT STATUS UPDATE
# =====================================================

def update_alert_status(alert_id, new_status):

    allowed_statuses = [
        "OPEN",
        "INVESTIGATING",
        "RESOLVED"
    ]

    if new_status not in allowed_statuses:
        raise ValueError("Invalid alert status.")

    query = """
        MATCH (alert:FraudAlert {
            alert_id: $alert_id
        })

        SET
            alert.status = $new_status,
            alert.updated_at = datetime()

        RETURN
            alert.alert_id AS alert_id,
            alert.status AS status
    """

    with get_driver().session(
        database=NEO4J_DATABASE
    ) as session:

        result = session.execute_write(
            lambda tx: tx.run(
                query,
                alert_id=alert_id,
                new_status=new_status
            ).single()
        )

    return result.data() if result else None


# =====================================================
# 6. HEADER
# =====================================================

st.markdown(
    '<div class="main-title">📊 FinGraph</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div class="subtitle">'
    'Real-Time Financial Fraud Intelligence & Monitoring System'
    '</div>',
    unsafe_allow_html=True
)

st.divider()


# =====================================================
# 7. SIDEBAR FILTERS
# =====================================================

st.sidebar.title("⚙️ Control Center")
st.sidebar.caption("Dashboard Filters")
st.sidebar.markdown("---")

start_date = st.sidebar.date_input(
    "Start Date",
    value=date.today() - timedelta(days=30),
    key="start_date"
)

end_date = st.sidebar.date_input(
    "End Date",
    value=date.today(),
    key="end_date"
)

if start_date > end_date:
    st.sidebar.error("Start date cannot be after end date.")
    st.stop()

channels = st.sidebar.multiselect(
    "Transaction Channels",
    ["UPI", "NEFT", "IMPS", "RTGS", "CARD"],
    default=["UPI", "NEFT", "IMPS", "RTGS", "CARD"]
)

risk_levels = st.sidebar.multiselect(
    "Risk Levels",
    ["HIGH", "MEDIUM", "LOW"],
    default=["HIGH", "MEDIUM", "LOW"]
)

account_search = st.sidebar.text_input(
    "Search Account",
    placeholder="Example: A0066"
).strip().upper()

graph_limit = st.sidebar.slider(
    "Network Graph Transactions",
    min_value=20,
    max_value=200,
    value=60,
    step=20
)

st.sidebar.markdown("---")
st.sidebar.caption("FinGraph | Streaming Analytics")

if st.sidebar.button(
    "🔄 Refresh Now",
    use_container_width=True
):
    st.rerun()


# =====================================================
# 8. DASHBOARD CONTENT
# =====================================================

@st.fragment(run_every="5s")
def dashboard_content(
    start_date,
    end_date,
    channels,
    risk_levels,
    account_search,
    graph_limit
):

    params = {
        "start_date": str(start_date),
        "end_date": str(end_date),
        "channels": channels,
        "account_search": account_search
    }

    try:

        # ---------------------------------------------
        # A. DATABASE STATUS
        # ---------------------------------------------

        get_driver().verify_connectivity()

        status_col1, status_col2 = st.columns([3, 1])

        with status_col1:
            st.markdown(
                '<span class="status-live">● LIVE DATABASE CONNECTED</span>',
                unsafe_allow_html=True
            )

        with status_col2:
            st.caption("Auto-refresh: 5 seconds")

        # ---------------------------------------------
        # B. KPI METRICS
        # ---------------------------------------------

        metrics_data = run_query("""
            MATCH (a:Account)

            WITH
                count(a) AS total_accounts,
                count(
                    CASE WHEN a.risk_level = 'HIGH'
                    THEN 1 END
                ) AS high_risk

            OPTIONAL MATCH ()-[r:TRANSFERRED_TO]->()

            WITH
                total_accounts,
                high_risk,
                count(r) AS total_transactions

            OPTIONAL MATCH (alert:FraudAlert {
                status: 'OPEN'
            })

            RETURN
                total_accounts,
                high_risk,
                total_transactions,
                count(alert) AS open_alerts
        """)

        metrics = metrics_data[0]

        filtered_count_data = run_query("""
            MATCH (a:Account)-[r:TRANSFERRED_TO]->(b:Account)

            WHERE date(datetime(r.timestamp))
                      >= date($start_date)
              AND date(datetime(r.timestamp))
                      <= date($end_date)
              AND (
                    size($channels) = 0
                    OR r.channel IN $channels
              )
              AND (
                    $account_search = ''
                    OR toUpper(a.account_id) CONTAINS $account_search
                    OR toUpper(b.account_id) CONTAINS $account_search
              )

            RETURN count(r) AS filtered_transactions
        """, params)

        filtered_transactions = (
            filtered_count_data[0]["filtered_transactions"]
        )

        st.markdown(
            '<div class="section-title">Executive Overview</div>',
            unsafe_allow_html=True
        )

        k1, k2, k3, k4 = st.columns(4)

        k1.metric(
            "Total Transactions",
            f"{metrics['total_transactions']:,}"
        )

        k2.metric(
            "Total Accounts",
            f"{metrics['total_accounts']:,}"
        )

        k3.metric(
            "High-Risk Accounts",
            f"{metrics['high_risk']:,}"
        )

        k4.metric(
            "Open Fraud Alerts",
            f"{metrics['open_alerts']:,}"
        )

        st.caption(
            f"Transactions matching current filters: "
            f"{filtered_transactions:,}"
        )

        st.divider()

        # ---------------------------------------------
        # C. TABS
        # ---------------------------------------------

        (
            overview_tab,
            fraud_tab,
            network_tab,
            explorer_tab,
            account_tab
        ) = st.tabs([
            "📈 Overview",
            "🚨 Fraud Intelligence",
            "🕸️ Network Graph",
            "💳 Transaction Explorer",
            "👤 Account 360° View"
        ])

        # =================================================
        # TAB 1: OVERVIEW
        # =================================================

        with overview_tab:

            st.subheader("Transaction Analytics")

            trend_data = run_query("""
                MATCH (a:Account)-[r:TRANSFERRED_TO]->(b:Account)

                WHERE date(datetime(r.timestamp))
                          >= date($start_date)
                  AND date(datetime(r.timestamp))
                          <= date($end_date)
                  AND (
                        size($channels) = 0
                        OR r.channel IN $channels
                  )
                  AND (
                        $account_search = ''
                        OR toUpper(a.account_id) CONTAINS $account_search
                        OR toUpper(b.account_id) CONTAINS $account_search
                  )

                RETURN
                    date(datetime(r.timestamp)) AS day,
                    count(r) AS transactions,
                    sum(toFloat(r.amount)) AS amount

                ORDER BY day
            """, params)

            trend_df = make_dataframe(trend_data)

            if not trend_df.empty:

                trend_df["day"] = pd.to_datetime(
                    trend_df["day"].astype(str)
                )

                left, right = st.columns(2)

                with left:

                    fig = px.line(
                        trend_df,
                        x="day",
                        y="transactions",
                        markers=True,
                        title="Daily Transaction Volume",
                        labels={
                            "day": "Date",
                            "transactions": "Transactions"
                        }
                    )

                    st.plotly_chart(
                        apply_plotly_theme(fig),
                        use_container_width=True
                    )

                with right:

                    fig = px.area(
                        trend_df,
                        x="day",
                        y="amount",
                        title="Daily Transaction Value",
                        labels={
                            "day": "Date",
                            "amount": "Transaction Amount (₹)"
                        }
                    )

                    st.plotly_chart(
                        apply_plotly_theme(fig),
                        use_container_width=True
                    )

            else:
                st.info("No transactions found for selected filters.")

            st.subheader("Account Risk Distribution")

            risk_data = run_query("""
                MATCH (a:Account)

                RETURN
                    a.risk_level AS risk_level,
                    count(a) AS accounts

                ORDER BY risk_level
            """)

            risk_df = make_dataframe(risk_data)

            if not risk_df.empty:

                left, right = st.columns(2)

                with left:

                    fig = px.pie(
                        risk_df,
                        names="risk_level",
                        values="accounts",
                        hole=0.55,
                        title="Risk Level Breakdown"
                    )

                    st.plotly_chart(
                        apply_plotly_theme(fig),
                        use_container_width=True
                    )

                with right:

                    fig = px.bar(
                        risk_df,
                        x="risk_level",
                        y="accounts",
                        color="risk_level",
                        title="Accounts by Risk Level",
                        labels={
                            "risk_level": "Risk Level",
                            "accounts": "Accounts"
                        }
                    )

                    st.plotly_chart(
                        apply_plotly_theme(fig),
                        use_container_width=True
                    )

        # =================================================
        # TAB 2: FRAUD INTELLIGENCE + ALERT MANAGEMENT
        # =================================================

        with fraud_tab:

            st.subheader("🚨 Fraud Alert Monitoring")

            alert_data = run_query("""
                MATCH (a:Account)-[:GENERATED_ALERT]->(
                    alert:FraudAlert
                )

                WHERE (
                    size($risk_levels) = 0
                    OR alert.risk_level IN $risk_levels
                )
                AND (
                    $account_search = ''
                    OR toUpper(a.account_id) CONTAINS $account_search
                )

                RETURN
                    alert.alert_id AS alert_id,
                    a.account_id AS account,
                    alert.risk_score AS risk_score,
                    alert.risk_level AS risk_level,
                    alert.alert_type AS alert_type,
                    alert.triggering_transaction_id AS transaction_id,
                    toString(alert.created_at) AS created_at,
                    alert.status AS status

                ORDER BY alert.created_at DESC
                LIMIT 100
            """, {
                "risk_levels": risk_levels,
                "account_search": account_search
            })

            alert_df = make_dataframe(alert_data)

            if not alert_df.empty:

                # ALERT SUMMARY

                status_counts = (
                    alert_df["status"]
                    .fillna("UNKNOWN")
                    .value_counts()
                )

                st.markdown("#### Alert Status Summary")

                s1, s2, s3 = st.columns(3)

                s1.metric(
                    "Open",
                    int(status_counts.get("OPEN", 0))
                )

                s2.metric(
                    "Investigating",
                    int(status_counts.get("INVESTIGATING", 0))
                )

                s3.metric(
                    "Resolved",
                    int(status_counts.get("RESOLVED", 0))
                )

                st.divider()

                st.subheader("All Fraud Alerts")

                st.dataframe(
                    alert_df,
                    use_container_width=True,
                    hide_index=True
                )

                csv = alert_df.to_csv(
                    index=False
                ).encode("utf-8")

                st.download_button(
                    "⬇️ Download Fraud Alerts CSV",
                    data=csv,
                    file_name="fingraph_fraud_alerts.csv",
                    mime="text/csv"
                )

            else:

                st.info("No matching fraud alerts found.")

            # ---------------------------------------------
            # FRAUD ALERT MANAGEMENT
            # ---------------------------------------------

            st.divider()

            st.subheader("🛠️ Fraud Alert Management")

            if not alert_df.empty:

                alert_options = (
                    alert_df["alert_id"]
                    .dropna()
                    .tolist()
                )

                selected_alert_id = st.selectbox(
                    "Select an Alert to Investigate",
                    alert_options,
                    key="selected_alert_management"
                )

                selected_alert = alert_df[
                    alert_df["alert_id"] == selected_alert_id
                ].iloc[0]

                st.markdown("#### Selected Alert Details")

                detail_col1, detail_col2, detail_col3 = st.columns(3)

                detail_col1.metric(
                    "Account",
                    selected_alert["account"]
                )

                detail_col2.metric(
                    "Risk Score",
                    f"{selected_alert['risk_score']}/100"
                )

                detail_col3.metric(
                    "Current Status",
                    selected_alert["status"] or "UNKNOWN"
                )

                st.write(
                    "**Alert Type:**",
                    selected_alert["alert_type"]
                )

                st.write(
                    "**Triggering Transaction:**",
                    selected_alert["transaction_id"]
                )

                st.write(
                    "**Created At:**",
                    selected_alert["created_at"]
                )

                st.markdown("#### Update Alert Status")

                status_options = [
                    "OPEN",
                    "INVESTIGATING",
                    "RESOLVED"
                ]

                current_status = selected_alert["status"]

                if current_status not in status_options:
                    current_status = "OPEN"

                new_status = st.selectbox(
                    "Choose New Status",
                    status_options,
                    index=status_options.index(current_status),
                    key=f"new_status_{selected_alert_id}"
                )

                if st.button(
                    "💾 Update Alert Status",
                    use_container_width=True,
                    key=f"update_{selected_alert_id}"
                ):

                    if new_status == current_status:

                        st.info(
                            "Alert already has this status."
                        )

                    else:

                        try:

                            updated_alert = update_alert_status(
                                selected_alert_id,
                                new_status
                            )

                            if updated_alert:

                                st.success(
                                    f"Alert {selected_alert_id} "
                                    f"updated to {new_status}."
                                )

                                st.rerun()

                            else:

                                st.error(
                                    "Alert not found in Neo4j."
                                )

                        except Exception as error:

                            st.error(
                                f"Status update failed: {error}"
                            )

            else:

                st.info(
                    "No alerts available for management."
                )

            # ---------------------------------------------
            # HIGH-RISK ACCOUNT DETAILS
            # ---------------------------------------------

            st.divider()

            st.subheader("High-Risk Account Details")

            high_risk_data = run_query("""
                MATCH (a:Account)

                WHERE a.risk_level = 'HIGH'
                  AND (
                        $account_search = ''
                        OR toUpper(a.account_id) CONTAINS $account_search
                  )

                RETURN
                    a.account_id AS account,
                    a.risk_score AS risk_score,
                    a.risk_level AS risk_level

                ORDER BY a.risk_score DESC
                LIMIT 100
            """, {
                "account_search": account_search
            })

            high_risk_df = make_dataframe(high_risk_data)

            if not high_risk_df.empty:

                st.dataframe(
                    high_risk_df,
                    use_container_width=True,
                    hide_index=True
                )

            else:

                st.info("No matching high-risk accounts.")

        # =================================================
        # TAB 3: INTERACTIVE NETWORK GRAPH
        # =================================================

        with network_tab:

            st.subheader("🕸️ Account Transaction Network")

            st.caption(
                "Each node represents an account. "
                "Connections represent financial transfers."
            )

            graph_data = run_query("""
                MATCH (a:Account)-[r:TRANSFERRED_TO]->(b:Account)

                WHERE date(datetime(r.timestamp))
                          >= date($start_date)
                  AND date(datetime(r.timestamp))
                          <= date($end_date)
                  AND (
                        size($channels) = 0
                        OR r.channel IN $channels
                  )
                  AND (
                        $account_search = ''
                        OR toUpper(a.account_id) CONTAINS $account_search
                        OR toUpper(b.account_id) CONTAINS $account_search
                  )

                RETURN
                    a.account_id AS sender,
                    a.risk_level AS sender_risk,
                    b.account_id AS receiver,
                    b.risk_level AS receiver_risk,
                    count(r) AS transaction_count,
                    sum(toFloat(r.amount)) AS total_amount

                ORDER BY transaction_count DESC
                LIMIT $graph_limit
            """, {
                **params,
                "graph_limit": graph_limit
            })

            graph_df = make_dataframe(graph_data)

            if not graph_df.empty:

                G = nx.DiGraph()

                for _, row in graph_df.iterrows():

                    G.add_node(
                        row["sender"],
                        risk=row["sender_risk"]
                    )

                    G.add_node(
                        row["receiver"],
                        risk=row["receiver_risk"]
                    )

                    G.add_edge(
                        row["sender"],
                        row["receiver"],
                        amount=row["total_amount"],
                        count=row["transaction_count"]
                    )

                positions = nx.spring_layout(
                    G,
                    seed=42,
                    k=0.8,
                    iterations=50
                )

                edge_x = []
                edge_y = []

                for source, target in G.edges():

                    x0, y0 = positions[source]
                    x1, y1 = positions[target]

                    edge_x.extend([x0, x1, None])
                    edge_y.extend([y0, y1, None])

                edge_trace = go.Scatter(
                    x=edge_x,
                    y=edge_y,
                    mode="lines",
                    line=dict(
                        width=1,
                        color="#52647d"
                    ),
                    hoverinfo="none"
                )

                risk_colors = {
                    "HIGH": "#ef4444",
                    "MEDIUM": "#f59e0b",
                    "LOW": "#10b981"
                }

                node_x = []
                node_y = []
                node_text = []
                node_colors = []

                for node, attributes in G.nodes(data=True):

                    x, y = positions[node]

                    node_x.append(x)
                    node_y.append(y)

                    node_text.append(
                        f"Account: {node}<br>"
                        f"Risk Level: {attributes.get('risk', 'UNKNOWN')}"
                    )

                    node_colors.append(
                        risk_colors.get(
                            attributes.get("risk"),
                            "#94a3b8"
                        )
                    )

                node_trace = go.Scatter(
                    x=node_x,
                    y=node_y,
                    mode="markers+text",
                    text=list(G.nodes()),
                    textposition="top center",
                    hovertext=node_text,
                    hoverinfo="text",
                    marker=dict(
                        size=17,
                        color=node_colors,
                        line=dict(
                            width=1,
                            color="#e5e7eb"
                        )
                    )
                )

                fig = go.Figure(
                    data=[edge_trace, node_trace]
                )

                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="#151f31",
                    plot_bgcolor="#151f31",
                    height=650,
                    title="Interactive Account Relationship Graph",
                    showlegend=False,
                    hovermode="closest",
                    margin=dict(l=10, r=10, t=50, b=10),
                    xaxis=dict(
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False
                    ),
                    yaxis=dict(
                        showgrid=False,
                        zeroline=False,
                        showticklabels=False
                    )
                )

                st.plotly_chart(
                    fig,
                    use_container_width=True
                )

                st.caption(
                    "Red = HIGH | Orange = MEDIUM | Green = LOW"
                )

                st.caption(
                    "Graph shows the highest-activity connections "
                    "within the selected date and channel filters."
                )

            else:

                st.info(
                    "No network connections found for these filters."
                )

        # =================================================
        # TAB 4: TRANSACTION EXPLORER
        # =================================================

        with explorer_tab:

            st.subheader("💳 Transaction Explorer")

            transaction_data = run_query("""
                MATCH (a:Account)-[r:TRANSFERRED_TO]->(b:Account)

                WHERE date(datetime(r.timestamp))
                          >= date($start_date)
                  AND date(datetime(r.timestamp))
                          <= date($end_date)
                  AND (
                        size($channels) = 0
                        OR r.channel IN $channels
                  )
                  AND (
                        $account_search = ''
                        OR toUpper(a.account_id) CONTAINS $account_search
                        OR toUpper(b.account_id) CONTAINS $account_search
                  )

                RETURN
                    r.transaction_id AS transaction_id,
                    a.account_id AS sender,
                    b.account_id AS receiver,
                    r.amount AS amount,
                    r.currency AS currency,
                    r.channel AS channel,
                    r.scenario_label AS scenario,
                    r.is_synthetic_suspicious AS suspicious,
                    r.timestamp AS timestamp

                ORDER BY r.timestamp DESC
                LIMIT 500
            """, params)

            transaction_df = make_dataframe(transaction_data)

            if not transaction_df.empty:

                st.metric(
                    "Transactions Displayed",
                    len(transaction_df)
                )

                st.dataframe(
                    transaction_df,
                    use_container_width=True,
                    hide_index=True
                )

                csv = transaction_df.to_csv(
                    index=False
                ).encode("utf-8")

                st.download_button(
                    "⬇️ Download Transactions CSV",
                    data=csv,
                    file_name="fingraph_transactions.csv",
                    mime="text/csv"
                )

                st.divider()

                st.subheader("Transaction Summary")

                total_value = pd.to_numeric(
                    transaction_df["amount"],
                    errors="coerce"
                ).fillna(0).sum()

                summary_col1, summary_col2 = st.columns(2)

                summary_col1.metric(
                    "Filtered Transaction Value",
                    format_currency(total_value)
                )

                summary_col2.metric(
                    "Displayed Transaction Count",
                    f"{len(transaction_df):,}"
                )

            else:

                st.info(
                    "No transactions found for selected filters."
                )

        # =================================================
        # TAB 5: ACCOUNT 360° VIEW
        # =================================================

        with account_tab:

            st.subheader("👤 Account 360° Investigation")

            st.caption(
                "Complete financial profile, transaction behaviour "
                "and risk analysis of an individual account."
            )

            # A. AVAILABLE ACCOUNTS

            account_options_data = run_query("""
                MATCH (a:Account)
                RETURN a.account_id AS account
                ORDER BY account
            """)

            account_options = [
                item["account"]
                for item in account_options_data
            ]

            if not account_options:

                st.warning("No accounts found in Neo4j.")

            else:

                default_index = 0

                if account_search:

                    matching_accounts = [
                        i for i, account in enumerate(account_options)
                        if account_search in account.upper()
                    ]

                    if matching_accounts:
                        default_index = matching_accounts[0]

                selected_account = st.selectbox(
                    "Select Account ID",
                    account_options,
                    index=default_index,
                    key="account_360_selector"
                )

                st.divider()

                # B. ACCOUNT PROFILE

                profile_data = run_query("""
                    MATCH (a:Account {
                        account_id: $account_id
                    })

                    RETURN
                        a.account_id AS account,
                        coalesce(a.risk_score, 0) AS risk_score,
                        coalesce(a.risk_level, 'LOW') AS risk_level
                """, {
                    "account_id": selected_account
                })

                if profile_data:

                    profile = profile_data[0]

                    risk_score = profile["risk_score"]
                    risk_level = profile["risk_level"]

                    risk_color = {
                        "HIGH": "#ef4444",
                        "MEDIUM": "#f59e0b",
                        "LOW": "#10b981"
                    }.get(risk_level, "#94a3b8")

                    st.markdown(
                        f"""
                        <div style="
                            background:#151f31;
                            border:1px solid #29364b;
                            border-radius:12px;
                            padding:18px;
                            margin-bottom:18px;
                        ">
                            <div style="color:#9ca3af;font-size:13px;">
                                ACCOUNT PROFILE
                            </div>
                            <div style="
                                font-size:27px;
                                font-weight:700;
                                color:#f9fafb;
                            ">
                                {selected_account}
                            </div>
                            <div style="
                                margin-top:8px;
                                color:{risk_color};
                                font-weight:700;
                            ">
                                {risk_level} RISK
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    # C. INCOMING TRANSACTIONS

                    incoming_data = run_query("""
                        MATCH (a:Account {
                            account_id: $account_id
                        })

                        OPTIONAL MATCH (sender:Account)-[r:TRANSFERRED_TO]->(a)

                        RETURN
                            count(r) AS incoming_count,
                            coalesce(
                                sum(toFloat(r.amount)),
                                0
                            ) AS incoming_amount
                    """, {
                        "account_id": selected_account
                    })

                    # D. OUTGOING TRANSACTIONS

                    outgoing_data = run_query("""
                        MATCH (a:Account {
                            account_id: $account_id
                        })

                        OPTIONAL MATCH (a)-[r:TRANSFERRED_TO]->(receiver:Account)

                        RETURN
                            count(r) AS outgoing_count,
                            coalesce(
                                sum(toFloat(r.amount)),
                                0
                            ) AS outgoing_amount
                    """, {
                        "account_id": selected_account
                    })

                    incoming = incoming_data[0]
                    outgoing = outgoing_data[0]

                    incoming_count = incoming["incoming_count"]
                    outgoing_count = outgoing["outgoing_count"]

                    incoming_amount = incoming["incoming_amount"]
                    outgoing_amount = outgoing["outgoing_amount"]

                    total_count = incoming_count + outgoing_count

                    # E. KPI CARDS

                    st.subheader("Financial Activity")

                    c1, c2, c3, c4 = st.columns(4)

                    c1.metric(
                        "Risk Score",
                        f"{risk_score}/100"
                    )

                    c2.metric(
                        "Total Transactions",
                        f"{total_count:,}"
                    )

                    c3.metric(
                        "Money Received",
                        format_currency(incoming_amount)
                    )

                    c4.metric(
                        "Money Sent",
                        format_currency(outgoing_amount)
                    )

                    st.divider()

                    # F. TRANSACTION DIRECTION ANALYSIS

                    st.subheader("Transaction Direction Analysis")

                    left, right = st.columns(2)

                    with left:
                        st.metric(
                            "Incoming Transactions",
                            f"{incoming_count:,}"
                        )

                    with right:
                        st.metric(
                            "Outgoing Transactions",
                            f"{outgoing_count:,}"
                        )

                    direction_df = pd.DataFrame({
                        "Direction": ["Incoming", "Outgoing"],
                        "Transactions": [
                            incoming_count,
                            outgoing_count
                        ],
                        "Amount": [
                            incoming_amount,
                            outgoing_amount
                        ]
                    })

                    if total_count > 0:

                        chart_left, chart_right = st.columns(2)

                        with chart_left:

                            fig = px.pie(
                                direction_df,
                                names="Direction",
                                values="Transactions",
                                hole=0.55,
                                title="Transaction Count"
                            )

                            st.plotly_chart(
                                apply_plotly_theme(fig),
                                use_container_width=True
                            )

                        with chart_right:

                            fig = px.bar(
                                direction_df,
                                x="Direction",
                                y="Amount",
                                color="Direction",
                                title="Money Flow",
                                labels={
                                    "Amount": "Amount (₹)",
                                    "Direction": "Transaction Direction"
                                }
                            )

                            st.plotly_chart(
                                apply_plotly_theme(fig),
                                use_container_width=True
                            )

                    # G. CONNECTED ACCOUNTS

                    st.divider()

                    st.subheader("🔗 Connected Accounts")

                    connected_data = run_query("""
                        MATCH (a:Account {
                            account_id: $account_id
                        })

                        CALL {
                            WITH a

                            MATCH (a)-[r:TRANSFERRED_TO]->(other:Account)

                            RETURN
                                other.account_id AS connected_account,
                                'OUTGOING' AS direction,
                                count(r) AS transaction_count,
                                sum(toFloat(r.amount)) AS total_amount

                            UNION ALL

                            WITH a

                            MATCH (other:Account)-[r:TRANSFERRED_TO]->(a)

                            RETURN
                                other.account_id AS connected_account,
                                'INCOMING' AS direction,
                                count(r) AS transaction_count,
                                sum(toFloat(r.amount)) AS total_amount
                        }

                        RETURN
                            connected_account,
                            direction,
                            transaction_count,
                            total_amount

                        ORDER BY transaction_count DESC
                        LIMIT 50
                    """, {
                        "account_id": selected_account
                    })

                    connected_df = make_dataframe(connected_data)

                    if not connected_df.empty:

                        st.dataframe(
                            connected_df,
                            use_container_width=True,
                            hide_index=True
                        )

                    else:

                        st.info("No connected accounts found.")

                    # H. TRANSACTION HISTORY

                    st.divider()

                    st.subheader("📋 Account Transaction History")

                    history_data = run_query("""
                        MATCH (a:Account {
                            account_id: $account_id
                        })

                        CALL {
                            WITH a

                            MATCH (a)-[r:TRANSFERRED_TO]->(b:Account)

                            RETURN
                                r.transaction_id AS transaction_id,
                                a.account_id AS sender,
                                b.account_id AS receiver,
                                r.amount AS amount,
                                r.currency AS currency,
                                r.channel AS channel,
                                r.scenario_label AS scenario,
                                r.is_synthetic_suspicious AS suspicious,
                                r.timestamp AS timestamp,
                                'OUTGOING' AS direction

                            UNION ALL

                            WITH a

                            MATCH (b:Account)-[r:TRANSFERRED_TO]->(a)

                            RETURN
                                r.transaction_id AS transaction_id,
                                b.account_id AS sender,
                                a.account_id AS receiver,
                                r.amount AS amount,
                                r.currency AS currency,
                                r.channel AS channel,
                                r.scenario_label AS scenario,
                                r.is_synthetic_suspicious AS suspicious,
                                r.timestamp AS timestamp,
                                'INCOMING' AS direction
                        }

                        RETURN *
                        ORDER BY timestamp DESC
                        LIMIT 500
                    """, {
                        "account_id": selected_account
                    })

                    history_df = make_dataframe(history_data)

                    if not history_df.empty:

                        st.metric(
                            "Transactions Displayed",
                            len(history_df)
                        )

                        st.dataframe(
                            history_df,
                            use_container_width=True,
                            hide_index=True
                        )

                        history_csv = history_df.to_csv(
                            index=False
                        ).encode("utf-8")

                        st.download_button(
                            "⬇️ Download Account History",
                            data=history_csv,
                            file_name=f"{selected_account}_transactions.csv",
                            mime="text/csv",
                            key="account_history_download"
                        )

                    else:

                        st.info(
                            "No transaction history found for this account."
                        )

                else:

                    st.warning("Account profile not found.")

    except Exception as error:

        st.error(f"Dashboard Error: {error}")


# =====================================================
# 9. RUN DASHBOARD
# =====================================================

dashboard_content(
    start_date,
    end_date,
    channels,
    risk_levels,
    account_search,
    graph_limit
)

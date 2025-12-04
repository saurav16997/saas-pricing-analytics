import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os

# ==========================================
# 1. CONFIGURATION & SETUP
# ==========================================
st.set_page_config(
    page_title="SaaS Pricing Intelligence", 
    layout="wide", 
    page_icon="🚀",
    initial_sidebar_state="expanded"
)

# Robust database path finding
# This calculates the path relative to this file to find data/saas_analytics.db
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
DB_PATH = os.path.join(project_root, "data", "saas_analytics.db")

# Professional CSS for "Executive Dashboard" look
st.markdown("""
<style>
    .metric-card {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .stMetricLabel {
        color: #666;
        font-size: 14px !important;
        font-weight: 500;
    }
    .stMetricValue {
        color: #2c3e50;
        font-size: 28px !important;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. DATA LOADING HELPER
# ==========================================
@st.cache_data
def load_data(query):
    """Safely loads data from SQLite with error handling"""
    if not os.path.exists(DB_PATH):
        st.error(f"❌ Database not found at `{DB_PATH}`. Please run this from the project root.")
        return pd.DataFrame()
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()

# ==========================================
# 3. SIDEBAR CONTROLS
# ==========================================
st.sidebar.title("⚙️ Market Filters")
st.sidebar.caption("Slice the data to find specific insights.")

# Load Companies for Filter
companies_df = load_data("SELECT DISTINCT company_name FROM companies ORDER BY company_name")
if not companies_df.empty:
    all_companies = companies_df['company_name'].tolist()
    # Default to top 5 companies if available
    default_companies = all_companies[:5] if len(all_companies) >= 5 else all_companies
    
    selected_companies = st.sidebar.multiselect(
        "Select Competitors", 
        all_companies,
        default=default_companies
    )
else:
    st.error("No company data found in database.")
    st.stop()

# Load Industries for Filter
industries_df = load_data("SELECT DISTINCT industry FROM users ORDER BY industry")
if not industries_df.empty:
    selected_industries = st.sidebar.multiselect(
        "Target Industries",
        industries_df['industry'].tolist(),
        default=industries_df['industry'].tolist()
    )
else:
    selected_industries = []

if not selected_companies:
    st.warning("Please select at least one company to view the dashboard.")
    st.stop()

# Dynamic SQL Filter Logic
# We construct these strings to inject into our WHERE clauses
company_list_str = "','".join(selected_companies)
company_filter = f"c.company_name IN ('{company_list_str}')"

if selected_industries:
    industry_list_str = "','".join(selected_industries)
    industry_filter = f"AND u.industry IN ('{industry_list_str}')"
else:
    industry_filter = "" # No industry filter applied

# ==========================================
# 4. EXECUTIVE SUMMARY (NORTH STAR METRICS)
# ==========================================
st.title("🚀 SaaS Competitive Landscape")
st.markdown("### 1. Executive Summary")

# KPI Query
kpi_sql = f"""
    SELECT 
        COUNT(DISTINCT u.user_id) as total_users,
        SUM(s.mrr) as total_mrr,
        AVG(s.mrr) as avg_arpu,
        (CAST(SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(u.user_id)) * 100 as churn_rate
    FROM users u
    JOIN subscriptions s ON u.user_id = s.user_id
    JOIN companies c ON u.company_id = c.company_id
    WHERE {company_filter} {industry_filter}
"""
df_kpi = load_data(kpi_sql)

if not df_kpi.empty and df_kpi['total_users'][0] is not None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Active Market Size", f"{df_kpi['total_users'][0]:,.0f}", help="Total unique users in selection")
    col2.metric("Total Monthly Revenue", f"${df_kpi['total_mrr'][0]:,.0f}", help="Sum of MRR for all active subscriptions")
    col3.metric("Avg Revenue (ARPU)", f"${df_kpi['avg_arpu'][0]:.2f}", help="North Star Metric: Average Revenue Per User")
    col4.metric("Avg Churn Rate", f"{df_kpi['churn_rate'][0]:.2f}%", delta_color="inverse", help="Percentage of users who cancelled")
else:
    st.warning("No data matches your current filters.")

st.markdown("---")

# ==========================================
# 5. DETAILED ANALYSIS TABS
# ==========================================
tab1, tab2, tab3 = st.tabs(["🏆 Leaderboard", "🏭 Industry Strategy", "🧠 Pricing Psychology"])

# --- TAB 1: WHO IS WINNING? ---
with tab1:
    st.markdown("### Market Leadership Matrix")
    st.caption("Comparing Market Share (Volume) vs. Monetization Efficiency (Value)")
    
    leader_sql = f"""
        SELECT 
            c.company_name,
            COUNT(DISTINCT u.user_id) as users,
            AVG(s.mrr) as arpu,
            SUM(s.mrr) as total_mrr
        FROM companies c
        JOIN users u ON c.company_id = u.company_id
        JOIN subscriptions s ON u.user_id = s.user_id
        WHERE {company_filter} {industry_filter}
        GROUP BY 1
    """
    df_leader = load_data(leader_sql)
    
    if not df_leader.empty:
        # Scatter Plot: The best way to show "Niche vs Mass Market"
        fig_matrix = px.scatter(
            df_leader,
            x="users",
            y="arpu",
            size="total_mrr",
            color="company_name",
            text="company_name",
            title="Volume vs. Value Matrix",
            labels={"users": "Total Users (Market Share)", "arpu": "Avg Revenue Per User ($)"},
            height=500
        )
        st.plotly_chart(fig_matrix, use_container_width=True)
        st.info("💡 **How to read this:** Companies in the **Top-Right** are dominating (High Volume + High Price). Top-Left are 'Boutique' (High Price, Low Vol). Bottom-Right are 'Mass Market' (Low Price, High Vol).")

# --- TAB 2: INDUSTRY SEGMENTATION (Your Resume Highlight) ---
with tab2:
    st.markdown("### Strategic Segmentation")
    st.caption("Which industries are high-value (Green) vs. high-risk (Red)?")
    
    segment_sql = f"""
        SELECT 
            u.industry,
            AVG(s.mrr) as arpu,
            (CAST(SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(u.user_id)) * 100 as churn_rate
        FROM users u
        JOIN subscriptions s ON u.user_id = s.user_id
        JOIN companies c ON u.company_id = c.company_id
        WHERE {company_filter}
        GROUP BY 1
        ORDER BY arpu DESC
    """
    df_segment = load_data(segment_sql)
    
    if not df_segment.empty:
        # Bar Chart with Color Grading for Churn
        fig_seg = px.bar(
            df_segment,
            x="industry",
            y="arpu",
            color="churn_rate",
            color_continuous_scale="RdYlGn_r", # Red = High Churn (Bad), Green = Low Churn (Good)
            title="Industry Value vs. Retention Risk",
            labels={"arpu": "Revenue Per User ($)", "churn_rate": "Churn Rate (%)"}
        )
        st.plotly_chart(fig_seg, use_container_width=True)
        
        # Text Insight calculation
        best_ind = df_segment.iloc[0]['industry']
        worst_ind = df_segment.sort_values('churn_rate', ascending=False).iloc[0]['industry']
        
        col_text1, col_text2 = st.columns(2)
        col_text1.success(f"**Recommendation:** Focus sales efforts on **{best_ind}** (Highest Revenue).")
        col_text2.warning(f"**Warning:** **{worst_ind}** shows the highest churn risk. Investigate product-market fit.")

# --- TAB 3: PRICING ECONOMICS ---
with tab3:
    st.markdown("### Pricing Architecture Analysis")
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Price Elasticity")
        churn_sql = f"""
            SELECT 
                pt.price_numeric, 
                c.company_name,
                (CAST(SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(u.user_id)) * 100 as churn_rate
            FROM users u 
            JOIN subscriptions s ON u.user_id = s.user_id 
            JOIN pricing_tiers pt ON u.tier_id = pt.tier_id
            JOIN companies c ON u.company_id = c.company_id
            WHERE pt.price_numeric > 0 AND {company_filter} {industry_filter}
            GROUP BY 1, 2
        """
        df_churn = load_data(churn_sql)
        if not df_churn.empty:
            fig_elast = px.scatter(
                df_churn, 
                x="price_numeric", 
                y="churn_rate", 
                color="company_name",
                title="Does Price Impact Retention?"
            )
            st.plotly_chart(fig_elast, use_container_width=True)

    with col_b:
        st.subheader("The 'SSO Tax'")
        sso_sql = """
            SELECT 
                CASE WHEN has_sso = 1 THEN 'Enterprise (SSO)' ELSE 'Standard' END as type, 
                AVG(price_numeric) as price 
            FROM pricing_tiers 
            WHERE price_numeric > 0 
            GROUP BY 1
        """
        df_sso = load_data(sso_sql)
        if not df_sso.empty:
            fig_sso = px.bar(
                df_sso, 
                x="type", 
                y="price", 
                color="type", 
                title="Avg Price: With vs. Without SSO"
            )
            st.plotly_chart(fig_sso, use_container_width=True)
import sqlite3
import pandas as pd
import os

# ==========================================
# CONFIGURATION
# ==========================================
# We assume the script is run from the project root
DB_PATH = os.path.join("data", "saas_analytics.db")
OUTPUT_DIR = os.path.join("data", "analysis")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ==========================================
# ANALYTICAL QUERY LIBRARY
# ==========================================
QUERIES = {
    "1_revenue_efficiency": """
        SELECT 
            c.company_name,
            COUNT(DISTINCT u.user_id) as total_users,
            SUM(s.mrr) * 12 as total_arr,
            ROUND(AVG(s.mrr), 2) as arpu,
            ROUND(SUM(s.mrr) / NULLIF(COUNT(CASE WHEN s.mrr > 0 THEN 1 END), 0), 2) as arppu
        FROM companies c
        JOIN users u ON c.company_id = u.company_id
        JOIN subscriptions s ON u.user_id = s.user_id
        WHERE s.status = 'active'
        GROUP BY 1
        ORDER BY arpu DESC;
    """,
    
    "2_churn_vs_price": """
        SELECT 
            pt.tier_name,
            pt.price_numeric,
            c.company_name,
            COUNT(u.user_id) as total_subscribers,
            SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) as churned_users,
            ROUND(CAST(SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(u.user_id) * 100, 2) as churn_rate
        FROM pricing_tiers pt
        JOIN users u ON pt.tier_id = u.tier_id
        JOIN subscriptions s ON u.user_id = s.user_id
        JOIN companies c ON pt.company_id = c.company_id
        GROUP BY 1, 2, 3
        HAVING total_subscribers > 50
        ORDER BY pt.price_numeric ASC;
    """,

    "3_conversion_rates": """
        WITH user_counts AS (
            SELECT 
                c.company_name,
                COUNT(CASE WHEN pt.price_numeric = 0 THEN 1 END) as free_users,
                COUNT(CASE WHEN pt.price_numeric > 0 OR pt.price_numeric IS NULL THEN 1 END) as paid_users
            FROM companies c
            JOIN users u ON c.company_id = u.company_id
            JOIN pricing_tiers pt ON u.tier_id = pt.tier_id
            GROUP BY 1
        )
        SELECT 
            company_name,
            free_users,
            paid_users,
            ROUND(paid_users * 100.0 / NULLIF((free_users + paid_users), 0), 2) as conversion_rate
        FROM user_counts
        ORDER BY conversion_rate DESC;
    """,

    "4_sso_tax_feature_analysis": """
        SELECT 
            CASE WHEN pt.has_sso = 1 THEN 'Has SSO' ELSE 'No SSO' END as feature_presence,
            ROUND(AVG(pt.price_numeric), 2) as avg_price,
            ROUND(MIN(pt.price_numeric), 2) as min_entry_price
        FROM pricing_tiers pt
        WHERE pt.price_numeric > 0 
        GROUP BY 1;
    """,

    "5_market_segmentation": """
        SELECT 
            u.industry,
            COUNT(DISTINCT u.user_id) as total_users,
            ROUND(AVG(s.mrr), 2) as arpu,
            ROUND(CAST(SUM(CASE WHEN s.status = 'cancelled' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(u.user_id) * 100, 2) as churn_rate
        FROM users u
        JOIN subscriptions s ON u.user_id = s.user_id
        GROUP BY 1
        ORDER BY arpu DESC;
    """
}

def run_analysis():
    print(f"🔌 Connecting to database at: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: Database not found at {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        
        print("🚀 Starting Comparative Analysis...")
        print("-" * 50)

        for query_name, sql_query in QUERIES.items():
            print(f"   Running analysis: {query_name}...")
            
            # Execute query and load into Pandas DataFrame
            df = pd.read_sql_query(sql_query, conn)
            
            # Export to CSV
            output_file = os.path.join(OUTPUT_DIR, f"{query_name}.csv")
            df.to_csv(output_file, index=False)
            
            print(f"   ✅ Saved {len(df)} rows to {output_file}")
            
            # Print a sneak peek
            if not df.empty:
                print(f"   👉 Top result: {df.iloc[0].to_dict()}")
            print("-" * 50)

        conn.close()
        print("\n✨ Analysis Complete! All reports saved to data/analysis/")

    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    run_analysis()
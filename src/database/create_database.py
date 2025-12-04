"""
Database Creation Script
Loads all CSV data into SQLite database for analytical queries
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys

def create_connection(db_path):
    """Create database connection"""
    try:
        conn = sqlite3.connect(db_path)
        print(f"✅ Connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

def create_schema(conn):
    """Create database schema with proper relationships"""
    print("\n" + "="*70)
    print("CREATING DATABASE SCHEMA")
    print("="*70)
    
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    tables = ['events', 'subscriptions', 'users', 'pricing_tiers', 'companies']
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # 1. Companies table
    cursor.execute("""
        CREATE TABLE companies (
            company_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT UNIQUE NOT NULL,
            total_users INTEGER,
            active_users INTEGER,
            market_share REAL
        )
    """)
    print("✅ Created table: companies")
    
    # 2. Pricing tiers table
    cursor.execute("""
        CREATE TABLE pricing_tiers (
            tier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            tier_name TEXT NOT NULL,
            tier_level INTEGER NOT NULL,
            price_original TEXT,
            price_numeric REAL,
            pricing_model TEXT,
            feature_count INTEGER,
            has_ai BOOLEAN,
            has_sso BOOLEAN,
            has_api BOOLEAN,
            has_automation BOOLEAN,
            has_analytics BOOLEAN,
            has_integrations BOOLEAN,
            features TEXT,
            FOREIGN KEY (company_id) REFERENCES companies(company_id),
            UNIQUE(company_id, tier_name)
        )
    """)
    print("✅ Created table: pricing_tiers")
    
    # 3. Users table
    cursor.execute("""
        CREATE TABLE users (
            user_id TEXT PRIMARY KEY,
            company_id INTEGER NOT NULL,
            tier_id INTEGER NOT NULL,
            tier_name TEXT NOT NULL,
            tier_level INTEGER NOT NULL,
            signup_date DATE NOT NULL,
            status TEXT NOT NULL,
            country TEXT,
            industry TEXT,
            company_size TEXT,
            FOREIGN KEY (company_id) REFERENCES companies(company_id),
            FOREIGN KEY (tier_id) REFERENCES pricing_tiers(tier_id)
        )
    """)
    print("✅ Created table: users")
    
    # 4. Subscriptions table
    cursor.execute("""
        CREATE TABLE subscriptions (
            subscription_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            company_id INTEGER NOT NULL,
            tier_id INTEGER NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            mrr REAL NOT NULL,
            pricing_model TEXT,
            status TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (company_id) REFERENCES companies(company_id),
            FOREIGN KEY (tier_id) REFERENCES pricing_tiers(tier_id)
        )
    """)
    print("✅ Created table: subscriptions")
    
    # 5. Events table
    cursor.execute("""
        CREATE TABLE events (
            event_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            event_type TEXT NOT NULL,
            feature_used TEXT,
            session_duration_min REAL,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)
    print("✅ Created table: events")
    
    conn.commit()
    print("\n✅ Schema creation complete")

def create_indexes(conn):
    """Create indexes for faster queries"""
    print("\n" + "="*70)
    print("CREATING INDEXES")
    print("="*70)
    
    cursor = conn.cursor()
    
    indexes = [
        "CREATE INDEX idx_users_company ON users(company_id)",
        "CREATE INDEX idx_users_tier ON users(tier_id)",
        "CREATE INDEX idx_users_status ON users(status)",
        "CREATE INDEX idx_users_signup ON users(signup_date)",
        "CREATE INDEX idx_subs_user ON subscriptions(user_id)",
        "CREATE INDEX idx_subs_company ON subscriptions(company_id)",
        "CREATE INDEX idx_subs_status ON subscriptions(status)",
        "CREATE INDEX idx_subs_start ON subscriptions(start_date)",
        "CREATE INDEX idx_events_user ON events(user_id)",
        "CREATE INDEX idx_events_type ON events(event_type)",
        "CREATE INDEX idx_events_timestamp ON events(timestamp)",
        "CREATE INDEX idx_pricing_company ON pricing_tiers(company_id)"
    ]
    
    for idx_sql in indexes:
        cursor.execute(idx_sql)
        print(f"✅ {idx_sql.split('CREATE INDEX ')[1].split(' ON')[0]}")
    
    conn.commit()
    print("\n✅ All indexes created")

def load_companies(conn, pricing_df, users_df):
    """Load companies reference table"""
    print("\n" + "="*70)
    print("LOADING COMPANIES DATA")
    print("="*70)
    
    # Get unique companies
    companies = pricing_df['Company'].unique()
    
    company_data = []
    for company in companies:
        total_users = len(users_df[users_df['company'] == company])
        active_users = len(users_df[(users_df['company'] == company) & (users_df['status'] == 'active')])
        market_share = (total_users / len(users_df)) * 100
        
        company_data.append({
            'company_name': company,
            'total_users': total_users,
            'active_users': active_users,
            'market_share': round(market_share, 2)
        })
    
    companies_df = pd.DataFrame(company_data)
    companies_df.to_sql('companies', conn, if_exists='append', index=False)
    
    print(f"✅ Loaded {len(companies_df)} companies")
    print(companies_df.to_string(index=False))
    
    return companies_df

def load_pricing_tiers(conn, pricing_df):
    """Load pricing tiers with company references"""
    print("\n" + "="*70)
    print("LOADING PRICING TIERS DATA")
    print("="*70)
    
    # Get company_id mapping
    company_mapping = pd.read_sql("SELECT company_id, company_name FROM companies", conn)
    
    # Merge to get company_ids
    pricing_with_ids = pricing_df.merge(
        company_mapping, 
        left_on='Company', 
        right_on='company_name',
        how='left'
    )
    
    # Prepare data for insert
    pricing_insert = pricing_with_ids[[
        'company_id', 'Tier', 'Tier_Level', 'Price_Original', 'Price_Numeric',
        'Pricing_Model', 'Feature_Count', 'has_ai', 'has_sso', 'has_api',
        'has_automation', 'has_analytics', 'has_integrations', 'Features'
    ]].copy()
    
    pricing_insert.columns = [
        'company_id', 'tier_name', 'tier_level', 'price_original', 'price_numeric',
        'pricing_model', 'feature_count', 'has_ai', 'has_sso', 'has_api',
        'has_automation', 'has_analytics', 'has_integrations', 'features'
    ]
    
    pricing_insert.to_sql('pricing_tiers', conn, if_exists='append', index=False)
    
    print(f"✅ Loaded {len(pricing_insert)} pricing tiers")
    
    # Show summary
    tier_summary = pricing_insert.groupby('tier_level').size()
    print(f"\nTiers by level:")
    for level, count in tier_summary.items():
        print(f"   • Level {level}: {count} tiers")
    
    return pricing_insert

def load_users(conn, users_df):
    """Load users with proper foreign key references"""
    print("\n" + "="*70)
    print("LOADING USERS DATA")
    print("="*70)
    
    # Get company and tier mappings
    company_mapping = pd.read_sql("SELECT company_id, company_name FROM companies", conn)
    tier_mapping = pd.read_sql(
        "SELECT tier_id, company_id, tier_name FROM pricing_tiers", 
        conn
    )
    
    # Merge to get IDs
    users_with_company = users_df.merge(
        company_mapping,
        left_on='company',
        right_on='company_name',
        how='left'
    )
    
    users_with_ids = users_with_company.merge(
        tier_mapping,
        left_on=['company_id', 'tier'],
        right_on=['company_id', 'tier_name'],
        how='left'
    )
    
    # Check for missing mappings
    missing_tiers = users_with_ids['tier_id'].isna().sum()
    if missing_tiers > 0:
        print(f"⚠️  Warning: {missing_tiers} users with unmapped tiers")
        # Drop users without tier mappings
        users_with_ids = users_with_ids.dropna(subset=['tier_id'])
    
    # Prepare for insert
    users_insert = users_with_ids[[
        'user_id', 'company_id', 'tier_id', 'tier', 'tier_level',
        'signup_date', 'status', 'country', 'industry', 'company_size'
    ]].copy()
    
    users_insert.columns = [
        'user_id', 'company_id', 'tier_id', 'tier_name', 'tier_level',
        'signup_date', 'status', 'country', 'industry', 'company_size'
    ]
    
    # Insert in chunks for large dataset
    chunk_size = 10000
    total_chunks = len(users_insert) // chunk_size + 1
    
    for i in range(0, len(users_insert), chunk_size):
        chunk = users_insert.iloc[i:i+chunk_size]
        chunk.to_sql('users', conn, if_exists='append', index=False)
        chunk_num = (i // chunk_size) + 1
        print(f"   ✓ Loaded chunk {chunk_num}/{total_chunks} ({len(chunk):,} users)")
    
    print(f"\n✅ Loaded {len(users_insert):,} users total")
    
    return users_insert

def load_subscriptions(conn, subs_df):
    """Load subscriptions data"""
    print("\n" + "="*70)
    print("LOADING SUBSCRIPTIONS DATA")
    print("="*70)
    
    # Get mappings
    company_mapping = pd.read_sql("SELECT company_id, company_name FROM companies", conn)
    tier_mapping = pd.read_sql("SELECT tier_id, company_id, tier_name FROM pricing_tiers", conn)
    
    # Merge to get IDs
    subs_with_company = subs_df.merge(
        company_mapping,
        left_on='company',
        right_on='company_name',
        how='left'
    )
    
    subs_with_ids = subs_with_company.merge(
        tier_mapping,
        left_on=['company_id', 'tier'],
        right_on=['company_id', 'tier_name'],
        how='left'
    )
    
    # Check for missing
    missing = subs_with_ids['tier_id'].isna().sum()
    if missing > 0:
        print(f"⚠️  Warning: {missing} subscriptions with unmapped tiers")
        subs_with_ids = subs_with_ids.dropna(subset=['tier_id'])
    
    # Prepare insert
    subs_insert = subs_with_ids[[
        'subscription_id', 'user_id', 'company_id', 'tier_id',
        'start_date', 'end_date', 'mrr', 'pricing_model', 'status'
    ]].copy()
    
    # Insert in chunks
    chunk_size = 10000
    total_chunks = len(subs_insert) // chunk_size + 1
    
    for i in range(0, len(subs_insert), chunk_size):
        chunk = subs_insert.iloc[i:i+chunk_size]
        chunk.to_sql('subscriptions', conn, if_exists='append', index=False)
        chunk_num = (i // chunk_size) + 1
        print(f"   ✓ Loaded chunk {chunk_num}/{total_chunks} ({len(chunk):,} subscriptions)")
    
    print(f"\n✅ Loaded {len(subs_insert):,} subscriptions total")
    
    return subs_insert

def load_events(conn, events_df):
    """Load events data"""
    print("\n" + "="*70)
    print("LOADING EVENTS DATA")
    print("="*70)
    
    # Events only need user_id reference (already exists)
    events_insert = events_df[[
        'event_id', 'user_id', 'timestamp', 'event_type',
        'feature_used', 'session_duration_min'
    ]].copy()
    
    # Insert in chunks (500k rows)
    chunk_size = 50000
    total_chunks = len(events_insert) // chunk_size + 1
    
    for i in range(0, len(events_insert), chunk_size):
        chunk = events_insert.iloc[i:i+chunk_size]
        chunk.to_sql('events', conn, if_exists='append', index=False)
        chunk_num = (i // chunk_size) + 1
        print(f"   ✓ Loaded chunk {chunk_num}/{total_chunks} ({len(chunk):,} events)")
    
    print(f"\n✅ Loaded {len(events_insert):,} events total")
    
    return events_insert

def validate_database(conn):
    """Run validation checks"""
    print("\n" + "="*70)
    print("VALIDATING DATABASE")
    print("="*70)
    
    cursor = conn.cursor()
    
    # Check row counts
    tables = ['companies', 'pricing_tiers', 'users', 'subscriptions', 'events']
    
    print("\n📊 Table Row Counts:")
    for table in tables:
        count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"   • {table:20s}: {count:>10,} rows")
    
    # Check for orphaned records
    print("\n🔍 Foreign Key Integrity:")
    
    orphaned_users = cursor.execute("""
        SELECT COUNT(*) FROM users 
        WHERE company_id NOT IN (SELECT company_id FROM companies)
    """).fetchone()[0]
    print(f"   • Orphaned users: {orphaned_users}")
    
    orphaned_subs = cursor.execute("""
        SELECT COUNT(*) FROM subscriptions 
        WHERE user_id NOT IN (SELECT user_id FROM users)
    """).fetchone()[0]
    print(f"   • Orphaned subscriptions: {orphaned_subs}")
    
    orphaned_events = cursor.execute("""
        SELECT COUNT(*) FROM events 
        WHERE user_id NOT IN (SELECT user_id FROM users)
    """).fetchone()[0]
    print(f"   • Orphaned events: {orphaned_events}")
    
    if orphaned_users == 0 and orphaned_subs == 0 and orphaned_events == 0:
        print("\n✅ All foreign key constraints valid!")
    else:
        print("\n⚠️  Warning: Some orphaned records found")
    
    # Sample queries
    print("\n📊 Sample Analytics:")
    
    # MRR by company
    mrr_query = """
        SELECT 
            c.company_name,
            COUNT(s.subscription_id) as active_subs,
            ROUND(SUM(s.mrr), 2) as total_mrr,
            ROUND(AVG(s.mrr), 2) as avg_mrr
        FROM companies c
        JOIN subscriptions s ON c.company_id = s.company_id
        WHERE s.status = 'active'
        GROUP BY c.company_name
        ORDER BY total_mrr DESC
        LIMIT 5
    """
    
    print("\n   Top 5 Companies by MRR:")
    results = cursor.execute(mrr_query).fetchall()
    for row in results:
        print(f"      • {row[0]:12s}: ${row[2]:>10,.2f} MRR ({row[1]:>5,} users)")

def main():
    """Main execution"""
    print("\n" + "="*70)
    print("🚀 SAAS ANALYTICS DATABASE CREATION")
    print("="*70)
    print("\nLoading CSV data into SQLite database...\n")
    
    # Check if CSV files exist
    required_files = [
        "data/clean/pricing_standardized.csv",
        "data/synthetic/synthetic_users.csv",
        "data/synthetic/synthetic_subscriptions.csv",
        "data/synthetic/synthetic_events.csv"
    ]
    
    for file in required_files:
        if not Path(file).exists():
            print(f"❌ Error: Required file not found: {file}")
            sys.exit(1)
    
    # Load CSVs
    print("📂 Loading CSV files...")
    pricing_df = pd.read_csv("data/clean/pricing_standardized.csv")
    users_df = pd.read_csv("data/synthetic/synthetic_users.csv")
    subs_df = pd.read_csv("data/synthetic/synthetic_subscriptions.csv")
    events_df = pd.read_csv("data/synthetic/synthetic_events.csv")
    print("✅ All CSV files loaded\n")
    
    # Create database
    db_path = "data/saas_analytics.db"
    
    # Remove old database if exists
    if Path(db_path).exists():
        Path(db_path).unlink()
        print(f"🗑️  Removed old database\n")
    
    conn = create_connection(db_path)
    
    try:
        # Create schema
        create_schema(conn)
        
        # Load data in order (respecting foreign keys)
        companies_df = load_companies(conn, pricing_df, users_df)
        pricing_tiers_df = load_pricing_tiers(conn, pricing_df)
        users_loaded = load_users(conn, users_df)
        subs_loaded = load_subscriptions(conn, subs_df)
        events_loaded = load_events(conn, events_df)
        
        # Create indexes
        create_indexes(conn)
        
        # Validate
        validate_database(conn)
        
        print("\n" + "="*70)
        print("✅ DATABASE CREATION COMPLETE!")
        print("="*70)
        print(f"\n📁 Database location: {Path(db_path).absolute()}")
        print(f"💾 Database size: {Path(db_path).stat().st_size / 1024 / 1024:.2f} MB")
        print("\n🎯 Next steps:")
        print("   1. Write analytical SQL queries")
        print("   2. Build comparative analysis scripts")
        print("   3. Create dashboard visualizations\n")
        
    except Exception as e:
        print(f"\n❌ Error during database creation: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("🔒 Database connection closed\n")

if __name__ == "__main__":
    main()
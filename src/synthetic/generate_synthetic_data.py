"""
Synthetic User Data Generator for SaaS Analytics
Generates realistic user behavior, subscriptions, and events
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

def load_pricing_data():
    """Load the cleaned pricing data"""
    pricing_df = pd.read_csv("data/clean/pricing_standardized.csv")
    return pricing_df

def generate_users(n_users=100000):
    """
    Generate synthetic user records
    Simulates realistic distribution across companies and tiers
    """
    print("="*70)
    print("STEP 1: GENERATING USERS")
    print("="*70)
    
    # Load available companies and tiers
    pricing_df = load_pricing_data()
    
    # Company distribution (weighted by market presence)
    company_weights = {
        'Notion': 0.15,
        'Asana': 0.12,
        'Monday': 0.13,
        'Trello': 0.14,
        'Miro': 0.11,
        'Figma': 0.13,
        'Airtable': 0.10,
        'Clickup': 0.08,
        'Coda': 0.04
    }
    
    users = []
    
    print(f"\n🔄 Generating {n_users:,} users...")
    
    for i in range(n_users):
        # Select company
        company = random.choices(
            list(company_weights.keys()),
            weights=list(company_weights.values())
        )[0]
        
        # Get available tiers for this company
        company_tiers = pricing_df[pricing_df['Company'] == company]
        
        # Tier distribution (Free tier most common)
        tier_probs = {
            1: 0.60,  # Free tier - 60%
            2: 0.25,  # Basic/Pro tier - 25%
            3: 0.12,  # Business tier - 12%
            4: 0.03   # Enterprise tier - 3%
        }
        
        # Select tier level based on distribution
        tier_level = random.choices(
            list(tier_probs.keys()),
            weights=list(tier_probs.values())
        )[0]
        
        # Get actual tier name for this company and level
        available_tiers = company_tiers[company_tiers['Tier_Level'] == tier_level]
        
        if len(available_tiers) > 0:
            tier_row = available_tiers.iloc[0]
            tier = tier_row['Tier']
        else:
            # Fallback to first tier if level not available
            tier = company_tiers.iloc[0]['Tier']
        
        # Generate signup date (weighted towards recent months - growth trend)
        # Last 2 years of data
        days_ago = int(np.random.exponential(180))  # Exponential for recency bias
        days_ago = min(days_ago, 730)  # Cap at 2 years
        signup_date = datetime.now() - timedelta(days=days_ago)
        
        # User status based on tier (lower churn in higher tiers)
        churn_rates = {1: 0.40, 2: 0.25, 3: 0.15, 4: 0.05}
        is_churned = random.random() < churn_rates[tier_level]
        status = 'churned' if is_churned else 'active'
        
        # Geography distribution
        country = random.choices(
            ['United States', 'United Kingdom', 'Germany', 'Canada', 'Australia',
             'France', 'Netherlands', 'Singapore', 'India', 'Brazil'],
            weights=[0.40, 0.12, 0.10, 0.08, 0.06, 0.06, 0.05, 0.05, 0.05, 0.03]
        )[0]
        
        # Industry distribution
        industry = random.choices(
            ['Technology', 'Marketing', 'Consulting', 'Finance', 'Education',
             'Healthcare', 'Retail', 'Manufacturing', 'Non-profit', 'Other'],
            weights=[0.25, 0.18, 0.15, 0.10, 0.08, 0.07, 0.06, 0.05, 0.04, 0.02]
        )[0]
        
        # Company size (correlated with tier)
        if tier_level == 1:
            company_size = random.choices(['1-10', '11-50'], weights=[0.70, 0.30])[0]
        elif tier_level == 2:
            company_size = random.choices(['11-50', '51-200'], weights=[0.60, 0.40])[0]
        elif tier_level == 3:
            company_size = random.choices(['51-200', '201-1000'], weights=[0.50, 0.50])[0]
        else:  # Enterprise
            company_size = random.choices(['201-1000', '1000+'], weights=[0.40, 0.60])[0]
        
        users.append({
            'user_id': f'usr_{i+1:06d}',
            'company': company,
            'tier': tier,
            'tier_level': tier_level,
            'signup_date': signup_date.strftime('%Y-%m-%d'),
            'status': status,
            'country': country,
            'industry': industry,
            'company_size': company_size
        })
        
        if (i + 1) % 20000 == 0:
            print(f"   ✓ Generated {i+1:,} users...")
    
    users_df = pd.DataFrame(users)
    
    print(f"\n✅ Generated {len(users_df):,} users")
    print(f"   • Active: {(users_df['status'] == 'active').sum():,}")
    print(f"   • Churned: {(users_df['status'] == 'churned').sum():,}")
    print(f"\n📊 Distribution by tier level:")
    print(users_df['tier_level'].value_counts().sort_index())
    
    return users_df

def generate_subscriptions(users_df):
    """
    Generate subscription records linked to users
    Includes MRR calculation based on pricing
    """
    print("\n" + "="*70)
    print("STEP 2: GENERATING SUBSCRIPTIONS")
    print("="*70)
    
    pricing_df = load_pricing_data()
    subscriptions = []
    
    print(f"\n🔄 Creating subscriptions for {len(users_df):,} users...")
    
    for idx, user in users_df.iterrows():
        # Get price for this user's tier
        price_row = pricing_df[
            (pricing_df['Company'] == user['company']) & 
            (pricing_df['Tier'] == user['tier'])
        ]
        
        if len(price_row) > 0:
            price_numeric = price_row.iloc[0]['Price_Numeric']
            pricing_model = price_row.iloc[0]['Pricing_Model']
        else:
            price_numeric = 0
            pricing_model = 'Free'
        
        # Calculate MRR (Monthly Recurring Revenue)
        if pd.isna(price_numeric):  # Contact sales / Custom
            # Estimate enterprise pricing based on company size
            if user['company_size'] == '1000+':
                mrr = random.uniform(500, 2000)
            elif user['company_size'] == '201-1000':
                mrr = random.uniform(200, 800)
            else:
                mrr = random.uniform(50, 300)
        else:
            mrr = float(price_numeric)
        
        # Subscription dates
        start_date = datetime.strptime(user['signup_date'], '%Y-%m-%d')
        
        if user['status'] == 'churned':
            # Churned - set end date in the past
            tenure_days = random.randint(30, 365)
            end_date = start_date + timedelta(days=tenure_days)
            sub_status = 'cancelled'
        else:
            # Active - no end date
            end_date = None
            sub_status = 'active'
        
        subscriptions.append({
            'subscription_id': f'sub_{idx+1:06d}',
            'user_id': user['user_id'],
            'company': user['company'],
            'tier': user['tier'],
            'tier_level': user['tier_level'],
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d') if end_date else None,
            'mrr': round(mrr, 2),
            'pricing_model': pricing_model,
            'status': sub_status
        })
        
        if (idx + 1) % 20000 == 0:
            print(f"   ✓ Created {idx+1:,} subscriptions...")
    
    subs_df = pd.DataFrame(subscriptions)
    
    # Calculate MRR and ARR by company
    active_subs = subs_df[subs_df['status'] == 'active']
    
    mrr_by_company = active_subs.groupby('company')['mrr'].sum().sort_values(ascending=False)
    total_mrr = active_subs['mrr'].sum()
    total_arr = total_mrr * 12
    
    print(f"\n✅ Generated {len(subs_df):,} subscriptions")
    print(f"   • Active: {(subs_df['status'] == 'active').sum():,}")
    print(f"   • Cancelled: {(subs_df['status'] == 'cancelled').sum():,}")
    
    print(f"\n💰 Revenue Metrics (Market Total):")
    print(f"   • Total MRR: ${total_mrr:,.2f}")
    print(f"   • Total ARR: ${total_arr:,.2f}")
    print(f"   • Avg MRR per active user: ${total_mrr / len(active_subs):.2f}")
    
    print(f"\n💰 MRR by Company:")
    for company, mrr in mrr_by_company.items():
        arr = mrr * 12
        users = len(active_subs[active_subs['company'] == company])
        arpu = mrr / users if users > 0 else 0
        print(f"   • {company:12s}: ${mrr:>10,.2f} MRR | ${arr:>11,.2f} ARR | ${arpu:>6.2f} ARPU | {users:>5,} users")
    
    return subs_df

def generate_events(users_df, n_events=500000):
    """
    Generate user activity events
    Simulates feature usage patterns based on tier and engagement
    """
    print("\n" + "="*70)
    print("STEP 3: GENERATING USAGE EVENTS")
    print("="*70)
    
    pricing_df = load_pricing_data()
    
    # Event types and their frequency weights
    event_types = {
        'login': 0.30,
        'feature_usage': 0.35,
        'page_view': 0.20,
        'collaboration': 0.10,
        'export': 0.03,
        'admin_action': 0.02
    }
    
    # Feature categories from pricing data
    feature_types = [
        'ai_feature', 'automation', 'integration', 'sso', 
        'analytics', 'api_call', 'storage', 'collaboration'
    ]
    
    events = []
    
    print(f"\n🔄 Generating {n_events:,} events...")
    
    # Only generate events for active users
    active_users = users_df[users_df['status'] == 'active']
    
    # Weight events towards more engaged users (higher tiers)
    # Enterprise users generate more events
    user_weights = active_users['tier_level'].map({1: 1, 2: 2, 3: 3, 4: 5})
    user_probs = user_weights / user_weights.sum()
    
    for i in range(n_events):
        # Select user (weighted by engagement)
        user = active_users.sample(n=1, weights=user_probs).iloc[0]
        
        # Generate event timestamp (weighted towards recent dates)
        signup = datetime.strptime(user['signup_date'], '%Y-%m-%d')
        days_since_signup = (datetime.now() - signup).days
        
        # Random day within user's tenure, weighted towards recent
        days_offset = int(np.random.exponential(days_since_signup / 3))
        days_offset = min(days_offset, days_since_signup)
        
        event_date = signup + timedelta(days=days_offset)
        
        # Add random time (business hours weighted)
        hour = int(np.random.normal(13, 3))  # Peak around 1pm
        hour = max(8, min(18, hour))  # Clamp to 8am-6pm
        minute = random.randint(0, 59)
        
        event_timestamp = event_date.replace(hour=hour, minute=minute)
        
        # Select event type
        event_type = random.choices(
            list(event_types.keys()),
            weights=list(event_types.values())
        )[0]
        
        # Get user's available features
        user_features = pricing_df[
            (pricing_df['Company'] == user['company']) & 
            (pricing_df['Tier'] == user['tier'])
        ].iloc[0]
        
        # Select feature based on what's available to this tier
        if event_type == 'feature_usage':
            # Premium features only for higher tiers
            available_features = []
            if user_features['has_ai']:
                available_features.append('ai_feature')
            if user_features['has_automation']:
                available_features.append('automation')
            if user_features['has_integrations']:
                available_features.append('integration')
            if user_features['has_sso'] and user['tier_level'] >= 3:
                available_features.append('sso')
            if user_features['has_analytics']:
                available_features.append('analytics')
            if user_features['has_api'] and user['tier_level'] >= 3:
                available_features.append('api_call')
            
            # All tiers get basic features
            available_features.extend(['storage', 'collaboration'])
            
            feature_used = random.choice(available_features) if available_features else 'basic_feature'
        else:
            feature_used = None
        
        # Session duration (in minutes)
        # Higher tier users have longer sessions
        base_duration = {1: 5, 2: 12, 3: 20, 4: 30}[user['tier_level']]
        session_duration = abs(np.random.normal(base_duration, base_duration/2))
        
        events.append({
            'event_id': f'evt_{i+1:06d}',
            'user_id': user['user_id'],
            'timestamp': event_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'event_type': event_type,
            'feature_used': feature_used,
            'session_duration_min': round(session_duration, 1)
        })
        
        if (i + 1) % 100000 == 0:
            print(f"   ✓ Generated {i+1:,} events...")
    
    events_df = pd.DataFrame(events)
    
    print(f"\n✅ Generated {len(events_df):,} events")
    print(f"\n📊 Event type distribution:")
    print(events_df['event_type'].value_counts())
    
    return events_df

def save_synthetic_data(users_df, subs_df, events_df):
    """Save all synthetic data to files"""
    print("\n" + "="*70)
    print("STEP 4: SAVING SYNTHETIC DATA")
    print("="*70)
    
    output_dir = Path("data/synthetic")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save files
    users_path = output_dir / "synthetic_users.csv"
    subs_path = output_dir / "synthetic_subscriptions.csv"
    events_path = output_dir / "synthetic_events.csv"
    
    users_df.to_csv(users_path, index=False)
    subs_df.to_csv(subs_path, index=False)
    events_df.to_csv(events_path, index=False)
    
    print(f"\n✅ Saved synthetic data:")
    print(f"   • {users_path} ({len(users_df):,} rows)")
    print(f"   • {subs_path} ({len(subs_df):,} rows)")
    print(f"   • {events_path} ({len(events_df):,} rows)")
    
    # Generate summary stats
    print(f"\n📊 Summary Statistics:")
    print(f"   • Total users: {len(users_df):,}")
    print(f"   • Active users: {(users_df['status'] == 'active').sum():,}")
    print(f"   • Total events: {len(events_df):,}")
    print(f"   • Events per active user: {len(events_df) / (users_df['status'] == 'active').sum():.1f}")
    
    return users_path, subs_path, events_path

def main():
    """Main execution"""
    print("\n" + "="*70)
    print("🚀 SYNTHETIC DATA GENERATOR - SaaS Analytics")
    print("="*70)
    print("\nGenerating realistic user behavior data...\n")
    
    # Check if pricing data exists
    pricing_path = Path("data/clean/pricing_standardized.csv")
    if not pricing_path.exists():
        print("❌ Error: pricing_standardized.csv not found!")
        print("   Please run the ETL pipeline first: python src/etl/etl_pipeline.py")
        return
    
    # Generate data
    users_df = generate_users(n_users=100000)
    subs_df = generate_subscriptions(users_df)
    events_df = generate_events(users_df, n_events=500000)
    
    # Save data
    save_synthetic_data(users_df, subs_df, events_df)
    
    print("\n" + "="*70)
    print("✅ SYNTHETIC DATA GENERATION COMPLETE!")
    print("="*70)
    print("\n🎯 Next steps:")
    print("   1. Review the generated data in data/synthetic/")
    print("   2. Run analysis scripts to calculate KPIs")
    print("   3. Build dashboard for visualization")
    print("\n")

if __name__ == "__main__":
    main()
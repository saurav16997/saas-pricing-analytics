"""
ETL Pipeline: Raw → Staging → Clean
Transforms scraped SaaS pricing data into analysis-ready format
"""

import pandas as pd
import numpy as np
from pathlib import Path
import re

def combine_raw_to_staging():
    """
    Step 1: Combine all raw CSV files into single staging file
    """
    print("="*70)
    print("STEP 1: COMBINING RAW DATA → STAGING")
    print("="*70)
    
    raw_dir = Path("data/raw")
    staging_dir = Path("data/staging")
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all pricing CSV files
    csv_files = list(raw_dir.glob("*_pricing.csv"))
    
    if not csv_files:
        print("❌ No pricing CSV files found in data/raw/")
        return None
    
    print(f"\n📁 Found {len(csv_files)} raw files:")
    for f in csv_files:
        print(f"   • {f.name}")
    
    # Combine all files
    all_data = []
    
    for csv_file in csv_files:
        # Extract company name from filename
        company_name = csv_file.stem.replace("_pricing", "").capitalize()
        
        try:
            df = pd.read_csv(csv_file)
            
            # Add company column
            df.insert(0, 'Company', company_name)
            
            all_data.append(df)
            print(f"✅ Loaded {company_name}: {len(df)} tiers")
            
        except Exception as e:
            print(f"❌ Error loading {csv_file.name}: {e}")
            continue
    
    # Combine into single dataframe
    staging_df = pd.concat(all_data, ignore_index=True)
    
    # Save staging file
    output_path = staging_dir / "all_pricing_combined.csv"
    staging_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Combined data saved to: {output_path}")
    print(f"📊 Total rows: {len(staging_df)}")
    print(f"📊 Companies: {staging_df['Company'].nunique()}")
    
    return staging_df

def normalize_price(price_str):
    """
    Convert price string to numeric value
    Examples:
    - "Free" → 0
    - "$10" → 10
    - "$17.50" → 17.5
    - "Contact sales" → NaN
    """
    if pd.isna(price_str):
        return np.nan
    
    price_str = str(price_str).strip()
    
    # Handle "Free"
    if price_str.lower() == "free":
        return 0.0
    
    # Handle "Contact sales"
    if "contact" in price_str.lower():
        return np.nan
    
    # Extract numeric value
    match = re.search(r'\$?(\d+(?:\.\d{2})?)', price_str)
    if match:
        return float(match.group(1))
    
    return np.nan

def assign_tier_level(tier_name):
    """
    Assign numeric tier level for comparison
    1 = Free/Personal/Starter
    2 = Basic/Plus/Pro/Standard
    3 = Business/Advanced/Premium
    4 = Enterprise
    """
    tier_lower = tier_name.lower()
    
    if any(word in tier_lower for word in ['free', 'personal', 'starter']):
        return 1
    elif any(word in tier_lower for word in ['basic', 'plus', 'pro', 'standard', 'unlimited', 'team']):
        return 2
    elif any(word in tier_lower for word in ['business', 'advanced', 'premium', 'organization', 'professional']):
        return 3
    elif 'enterprise' in tier_lower:
        return 4
    else:
        return 2  # Default to mid-tier

def extract_feature_categories(features_str):
    """
    Categorize features into key areas
    Returns dict with boolean flags
    """
    if pd.isna(features_str) or features_str == "N/A":
        return {
            'has_ai': False,
            'has_sso': False,
            'has_api': False,
            'has_automation': False,
            'has_analytics': False,
            'has_integrations': False
        }
    
    features_lower = features_str.lower()
    
    return {
        'has_ai': bool(re.search(r'\bai\b|artificial intelligence|machine learning', features_lower)),
        'has_sso': bool(re.search(r'sso|single sign|saml', features_lower)),
        'has_api': bool(re.search(r'\bapi\b|enterprise api', features_lower)),
        'has_automation': bool(re.search(r'automat|workflow', features_lower)),
        'has_analytics': bool(re.search(r'analytic|reporting|dashboard', features_lower)),
        'has_integrations': bool(re.search(r'integrat|connect|sync', features_lower))
    }

def clean_and_normalize(staging_df):
    """
    Step 2: Clean and normalize staging data → production-ready
    """
    print("\n" + "="*70)
    print("STEP 2: CLEANING & NORMALIZING → PRODUCTION")
    print("="*70)
    
    df = staging_df.copy()
    
    print("\n🧹 Cleaning data...")
    
    # 1. Normalize prices
    df['Price_Original'] = df['Price']
    df['Price_Numeric'] = df['Price'].apply(normalize_price)
    print(f"   ✅ Normalized prices: {df['Price_Numeric'].notna().sum()}/{len(df)} have numeric values")
    
    # 2. Assign tier levels
    df['Tier_Level'] = df['Tier'].apply(assign_tier_level)
    print(f"   ✅ Assigned tier levels (1-4)")
    
    # 3. Extract feature categories
    print(f"   🔍 Extracting feature categories...")
    feature_categories = df['Features'].apply(extract_feature_categories)
    feature_df = pd.DataFrame(feature_categories.tolist())
    df = pd.concat([df, feature_df], axis=1)
    
    # 4. Count features
    df['Feature_Count'] = df['Features'].apply(
        lambda x: len(str(x).split('|')) if pd.notna(x) and x != 'N/A' else 0
    )
    print(f"   ✅ Counted features per tier")
    
    # 5. Add pricing model flag
    df['Pricing_Model'] = df['Price_Numeric'].apply(
        lambda x: 'Free' if x == 0 else 'Custom' if pd.isna(x) else 'Listed'
    )
    
    # Reorder columns for clarity
    column_order = [
        'Company', 'Tier', 'Tier_Level', 
        'Price_Original', 'Price_Numeric', 'Pricing_Model',
        'Feature_Count', 'has_ai', 'has_sso', 'has_api', 
        'has_automation', 'has_analytics', 'has_integrations',
        'Features'
    ]
    df = df[column_order]
    
    return df

def save_clean_data(clean_df):
    """
    Step 3: Save cleaned data to production folder
    """
    print("\n" + "="*70)
    print("STEP 3: SAVING CLEAN DATA")
    print("="*70)
    
    clean_dir = Path("data/clean")
    clean_dir.mkdir(parents=True, exist_ok=True)
    
    # Save main cleaned file
    output_path = clean_dir / "pricing_standardized.csv"
    clean_df.to_csv(output_path, index=False)
    print(f"\n✅ Saved: {output_path}")
    
    # Also save a summary file
    summary = clean_df.groupby('Company').agg({
        'Tier': 'count',
        'Price_Numeric': ['min', 'max', 'mean'],
        'Feature_Count': 'mean'
    }).round(2)
    
    summary.columns = ['Tier_Count', 'Min_Price', 'Max_Price', 'Avg_Price', 'Avg_Features']
    summary_path = clean_dir / "company_summary.csv"
    summary.to_csv(summary_path)
    print(f"✅ Saved: {summary_path}")
    
    return output_path

def generate_data_quality_report(clean_df):
    """
    Generate data quality report
    """
    print("\n" + "="*70)
    print("DATA QUALITY REPORT")
    print("="*70)
    
    total_rows = len(clean_df)
    
    print(f"\n📊 Overview:")
    print(f"   • Total records: {total_rows}")
    print(f"   • Companies: {clean_df['Company'].nunique()}")
    print(f"   • Unique tiers: {clean_df['Tier'].nunique()}")
    
    print(f"\n💰 Pricing Data:")
    print(f"   • With numeric prices: {clean_df['Price_Numeric'].notna().sum()} ({clean_df['Price_Numeric'].notna().sum()/total_rows*100:.1f}%)")
    print(f"   • Free tiers: {(clean_df['Price_Numeric'] == 0).sum()}")
    print(f"   • Custom pricing: {clean_df['Price_Numeric'].isna().sum()}")
    print(f"   • Price range: ${clean_df['Price_Numeric'].min():.0f} - ${clean_df['Price_Numeric'].max():.0f}")
    
    print(f"\n📋 Features Data:")
    print(f"   • With features: {(clean_df['Feature_Count'] > 0).sum()} ({(clean_df['Feature_Count'] > 0).sum()/total_rows*100:.1f}%)")
    print(f"   • Avg features per tier: {clean_df['Feature_Count'].mean():.1f}")
    
    print(f"\n🎯 Feature Categories:")
    for col in ['has_ai', 'has_sso', 'has_api', 'has_automation', 'has_analytics', 'has_integrations']:
        count = clean_df[col].sum()
        pct = count / total_rows * 100
        feature_name = col.replace('has_', '').upper()
        print(f"   • {feature_name}: {count} tiers ({pct:.1f}%)")
    
    print(f"\n📈 By Tier Level:")
    tier_summary = clean_df.groupby('Tier_Level').agg({
        'Price_Numeric': 'mean',
        'Feature_Count': 'mean'
    }).round(2)
    tier_summary.columns = ['Avg_Price', 'Avg_Features']
    print(tier_summary.to_string())

def main():
    """
    Main ETL pipeline execution
    """
    print("\n" + "="*70)
    print("🚀 SaaS PRICING ETL PIPELINE")
    print("="*70)
    print("\nTransforming raw scraped data → analysis-ready format\n")
    
    # Step 1: Combine raw files
    staging_df = combine_raw_to_staging()
    
    if staging_df is None:
        print("\n❌ Pipeline failed at staging step")
        return
    
    # Step 2: Clean and normalize
    clean_df = clean_and_normalize(staging_df)
    
    # Step 3: Save cleaned data
    output_path = save_clean_data(clean_df)
    
    # Step 4: Generate quality report
    generate_data_quality_report(clean_df)
    
    print("\n" + "="*70)
    print("✅ ETL PIPELINE COMPLETE!")
    print("="*70)
    print(f"\n📁 Output files:")
    print(f"   • data/staging/all_pricing_combined.csv")
    print(f"   • data/clean/pricing_standardized.csv")
    print(f"   • data/clean/company_summary.csv")
    print("\n🎯 Ready for analysis and dashboard creation!\n")

if __name__ == "__main__":
    main()
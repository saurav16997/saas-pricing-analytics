"""
Transform Script - Cleans and standardizes raw pricing data
Reads from data/raw/ → Outputs to data/staging/
"""

import pandas as pd
from pathlib import Path
import re

def standardize_price(price_str):
    """
    Standardize price formats to consistent values
    
    Examples:
        "$10" -> 10.0
        "Free" -> 0.0
        "Contact sales" -> None
        "$10/month" -> 10.0
    """
    if pd.isna(price_str):
        return None
    
    price_str = str(price_str).strip().lower()
    
    # Handle "Contact sales" or similar
    if any(keyword in price_str for keyword in ['contact', 'custom', 'enterprise']):
        return None
    
    # Handle "Free"
    if 'free' in price_str or price_str == '$0':
        return 0.0
    
    # Extract numeric value
    match = re.search(r'\$?(\d+(?:\.\d{2})?)', price_str)
    if match:
        return float(match.group(1))
    
    return None

def clean_features(features_str):
    """
    Clean feature text:
    - Remove excessive whitespace
    - Normalize pipe separators
    - Remove duplicates
    """
    if pd.isna(features_str) or features_str == 'N/A':
        return ''
    
    # Split on pipe, clean each feature
    features = [f.strip() for f in str(features_str).split('|')]
    
    # Remove empty strings and duplicates while preserving order
    seen = set()
    cleaned = []
    for feat in features:
        if feat and feat not in seen:
            seen.add(feat)
            cleaned.append(feat)
    
    return ' | '.join(cleaned)

def transform_company_data(input_path, company_name):
    """
    Transform a single company's pricing data
    
    Args:
        input_path: Path to raw CSV
        company_name: Name of the company
    
    Returns:
        Cleaned DataFrame
    """
    print(f"\n📊 Processing: {company_name}")
    
    # Read raw data
    df = pd.read_csv(input_path)
    print(f"   Rows: {len(df)}")
    
    # Add company column
    df['Company'] = company_name
    
    # Standardize price
    df['Price_Original'] = df['Price']  # Keep original for reference
    df['Price_Numeric'] = df['Price'].apply(standardize_price)
    
    # Clean features
    df['Features'] = df['Features'].apply(clean_features)
    
    # Reorder columns
    df = df[['Company', 'Tier', 'Price_Original', 'Price_Numeric', 'Features']]
    
    # Validation checks
    null_prices = df['Price_Original'].isna().sum()
    if null_prices > 0:
        print(f"   ⚠️ Warning: {null_prices} tiers have null prices")
    
    missing_features = (df['Features'] == '').sum()
    if missing_features > 0:
        print(f"   ⚠️ Warning: {missing_features} tiers have no features")
    
    print(f"   ✅ Transformed: {len(df)} tiers")
    
    return df

def main():
    """
    Main transform pipeline
    """
    print("="*70)
    print("🔄 SaaS Pricing Transform Pipeline")
    print("="*70)
    
    # Setup paths
    raw_dir = Path("data/raw")
    staging_dir = Path("data/staging")
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all raw CSV files
    raw_files = list(raw_dir.glob("*_pricing.csv"))
    
    if not raw_files:
        print("\n❌ No raw CSV files found in data/raw/")
        print("   Expected files like: notion_pricing.csv, asana_pricing.csv, etc.")
        return
    
    print(f"\n📁 Found {len(raw_files)} raw files")
    
    transformed_count = 0
    failed_files = []
    
    # Process each file
    for raw_file in raw_files:
        # Extract company name from filename (e.g., "notion_pricing.csv" -> "Notion")
        company_name = raw_file.stem.replace('_pricing', '').capitalize()
        
        try:
            # Transform
            df_cleaned = transform_company_data(raw_file, company_name)
            
            # Save to staging
            output_path = staging_dir / f"{company_name.lower()}_cleaned.csv"
            df_cleaned.to_csv(output_path, index=False)
            print(f"   💾 Saved: {output_path.name}")
            
            transformed_count += 1
            
        except Exception as e:
            print(f"   ❌ Error processing {company_name}: {str(e)}")
            failed_files.append(company_name)
            continue
    
    # Summary
    print("\n" + "="*70)
    print("📊 TRANSFORM SUMMARY")
    print("="*70)
    print(f"✅ Successfully transformed: {transformed_count}/{len(raw_files)}")
    
    if failed_files:
        print(f"❌ Failed: {len(failed_files)}")
        for name in failed_files:
            print(f"   - {name}")
    
    print(f"\n📁 Cleaned files saved to: {staging_dir.absolute()}")
    print("="*70)
    
    # Show sample of transformed data
    if transformed_count > 0:
        print("\n📋 Sample of transformed data:")
        sample_file = list(staging_dir.glob("*_cleaned.csv"))[0]
        sample_df = pd.read_csv(sample_file)
        print(f"\n{sample_file.name}:")
        print(sample_df.head(3).to_string(index=False))

if __name__ == "__main__":
    main()
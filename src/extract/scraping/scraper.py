"""
Universal SaaS Pricing Scraper
Scrapes pricing data (Tier, Price, Features) from multiple SaaS companies
"""

from selenium import webdriver 
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager 
import pandas as pd 
from pathlib import Path 
import time 
import re

def setup_driver():
    """Setup Chrome driver with anti-detection settings"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=chrome_options
    )
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.maximize_window()
    return driver

def extract_price(text, tier_name):
    """
    Extract price from text with intelligent pattern matching
    Returns: price string (e.g., "$10", "Free", "Contact sales")
    """
    text_lower = text.lower()
    
    # Priority 1: Contact sales patterns
    contact_patterns = ['contact sales', 'contact us', 'get in touch', 'talk to sales', 'custom pricing']
    if any(pattern in text_lower for pattern in contact_patterns):
        return "Contact sales"
    
    # Priority 2: Free patterns
    if tier_name.lower() == "free" or 'free forever' in text_lower or text_lower.strip() == 'free':
        return "Free"
    
    # Priority 3: Dollar amounts (most common)
    price_match = re.search(r'\$\d+(?:\.\d{2})?', text)
    if price_match:
        return price_match.group(0)
    
    # Priority 4: Number without dollar sign
    number_match = re.search(r'(\d+)(?:\.\d{2})?\s*(?:per|/|month|mo)', text_lower)
    if number_match:
        return f"${number_match.group(1)}"
    
    # Default: Enterprise usually means contact sales
    if tier_name.lower() == "enterprise":
        return "Contact sales"
    
    return "N/A"

def get_tier_names(company_name):
    """
    Return the correct tier names for each company
    Based on actual pricing page structures
    """
    tier_map = {
        "notion": ["Free", "Plus", "Business", "Enterprise"],
        "clickup": ["Free", "Unlimited", "Business", "Enterprise"],
        "asana": ["Personal", "Starter", "Advanced", "Enterprise"],
        "airtable": ["Free", "Team", "Business", "Enterprise"],
        "coda": ["Free", "Pro", "Team", "Enterprise"],
        "trello": ["Free", "Standard", "Premium", "Enterprise"],
        "figma": ["Starter", "Professional", "Organization", "Enterprise"],
        "monday": ["Free", "Basic", "Standard", "Pro", "Enterprise"],
        "linear": ["Free", "Standard", "Plus", "Enterprise"],
        "miro": ["Free", "Starter", "Business", "Enterprise"]
    }
    
    return tier_map.get(company_name.lower(), ["Free", "Pro", "Business", "Enterprise"])

def scrape_pricing(driver, url, company_name, tier_names):
    """
    Generic scraper that adapts to different SaaS pricing page structures
    
    Args:
        driver: Selenium WebDriver instance
        url: Pricing page URL
        company_name: Company name
        tier_names: List of tier names to search for
    
    Returns:
        List of dicts with Tier, Price, Features
    """
    print(f"\n{'='*70}")
    print(f"🎯 Scraping: {company_name}")
    print(f"{'='*70}")
    
    driver.get(url)
    print(f"📍 URL: {url}")
    
    # Wait for page load and scroll to trigger lazy-loaded content
    print("⏳ Loading page...")
    time.sleep(8)
    
    for i in range(4):
        driver.execute_script(f"window.scrollTo(0, {(i+1) * 700});")
        time.sleep(1)
    
    time.sleep(2)
    
    pricing_data = []
    
    for tier in tier_names:
        print(f"\n  🔍 Searching for: {tier}")
        try:
            # Find all elements containing the tier name
            tier_elements = driver.find_elements(
                By.XPATH, 
                f"//*[text()='{tier}' or contains(text(), '{tier}')]"
            )
            
            # Filter to find the actual heading (should be short text)
            tier_heading = None
            for elem in tier_elements:
                elem_text = elem.text.strip()
                if len(elem_text) < 50 and tier.lower() in elem_text.lower():
                    # Verify it's a heading-like element
                    tag = elem.tag_name.lower()
                    if tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span', 'div']:
                        tier_heading = elem
                        break
            
            if not tier_heading:
                print(f"     ❌ Heading not found")
                continue
            
            print(f"     ✅ Found heading")
            
            # Navigate up the DOM to find the pricing card container
            # Strategy: Find a container with pricing info but minimal other tier mentions
            current = tier_heading
            best_container = None
            best_score = -999
            
            for level in range(10):
                current = driver.execute_script("return arguments[0].parentElement;", current)
                
                try:
                    container_text = current.text
                    
                    # Skip if container is too small or too large
                    text_len = len(container_text)
                    if text_len < 50 or text_len > 5000:
                        continue
                    
                    # Count mentions of OTHER tiers (indicates we grabbed too much)
                    other_tiers = [t for t in tier_names if t.lower() != tier.lower()]
                    other_mentions = sum(1 for t in other_tiers if t in container_text)
                    
                    # Check for price indicators
                    has_price = bool(re.search(r'\$|free|contact', container_text, re.IGNORECASE))
                    
                    # Count feature items (list elements)
                    li_count = len(current.find_elements(By.TAG_NAME, "li"))
                    
                    # Scoring: reward price presence, penalize other tier mentions
                    score = 0
                    if has_price:
                        score += 50
                    score -= (other_mentions * 40)  # Penalize heavily for other tiers
                    if 3 <= li_count <= 30:  # Good feature count
                        score += 30
                    score -= (text_len / 200)  # Slight penalty for size
                    
                    if score > best_score:
                        best_score = score
                        best_container = current
                        
                except Exception:
                    continue
            
            if not best_container:
                print(f"     ⚠️ Could not find container")
                continue
            
            # Extract price
            container_text = best_container.text
            price = extract_price(container_text, tier)
            print(f"     💰 Price: {price}")
            
            # Extract features from list items
            features = []
            try:
                list_items = best_container.find_elements(By.TAG_NAME, "li")
                
                for li in list_items[:30]:  # Cap at 30 features
                    feature_text = li.text.strip()
                    
                    # Quality filters
                    if not feature_text:
                        continue
                    if len(feature_text) < 5:  # Too short
                        continue
                    if len(feature_text) > 250:  # Too long (probably not a feature)
                        continue
                    if feature_text in features:  # Duplicate
                        continue
                    
                    # Clean up the text
                    feature_text = feature_text.replace('\n', ' ').strip()
                    features.append(feature_text)
                
                print(f"     📋 Features: {len(features)} extracted")
                
            except Exception as e:
                print(f"     ⚠️ Feature extraction failed: {str(e)}")
            
            # Add to results
            pricing_data.append({
                "Tier": tier,
                "Price": price,
                "Features": " | ".join(features) if features else "N/A"
            })
            
        except Exception as e:
            print(f"     ❌ Error: {str(e)}")
            continue
    
    return pricing_data

def main():
    """
    Main execution: Read companies.csv and scrape all pricing pages
    """
    print("="*70)
    print("🚀 SaaS Pricing Scraper - Universal")
    print("="*70)
    
    # Read companies list
    companies_path = Path("docs/companies.csv")
    if not companies_path.exists():
        print("❌ Error: docs/companies.csv not found!")
        print("   Make sure you're running from the project root directory")
        return
    
    companies_df = pd.read_csv(companies_path)
    print(f"\n📋 Loaded {len(companies_df)} companies from companies.csv")
    
    # Setup output directory
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Output directory: {output_dir.absolute()}")
    
    # Initialize driver
    print("\n🔧 Setting up Chrome driver...")
    driver = setup_driver()
    
    # Track results
    results = []
    
    try:
        for idx, row in companies_df.iterrows():
            company = row['company']
            url = row['pricing_url']
            
            print(f"\n\n{'#'*70}")
            print(f"Company {idx+1}/{len(companies_df)}: {company}")
            print(f"{'#'*70}")
            
            try:
                # Get tier names for this company
                tier_names = get_tier_names(company)
                print(f"🎯 Looking for tiers: {', '.join(tier_names)}")
                
                # Scrape pricing data
                pricing_data = scrape_pricing(driver, url, company, tier_names)
                
                if pricing_data:
                    # Save to CSV
                    df = pd.DataFrame(pricing_data)
                    output_path = output_dir / f"{company.lower()}_pricing.csv"
                    df.to_csv(output_path, index=False)
                    
                    print(f"\n✅ SUCCESS: Saved {len(pricing_data)} tiers to {output_path.name}")
                    
                    # Show preview
                    print("\n📊 Data preview:")
                    for item in pricing_data:
                        feat_count = len(item['Features'].split('|')) if item['Features'] != 'N/A' else 0
                        print(f"   • {item['Tier']}: {item['Price']} ({feat_count} features)")
                    
                    results.append({
                        "Company": company,
                        "Status": "✅ Success",
                        "Tiers": len(pricing_data),
                        "File": output_path.name
                    })
                else:
                    print(f"\n⚠️ WARNING: No data extracted for {company}")
                    results.append({
                        "Company": company,
                        "Status": "⚠️ No data",
                        "Tiers": 0,
                        "File": "N/A"
                    })
                
                # Respectful delay between requests
                time.sleep(4)
                
            except Exception as e:
                error_msg = str(e)[:80]
                print(f"\n❌ FAILED: {company}")
                print(f"   Error: {error_msg}")
                
                results.append({
                    "Company": company,
                    "Status": f"❌ Error",
                    "Tiers": 0,
                    "File": "N/A"
                })
                continue
    
    finally:
        print("\n\n🔒 Closing browser...")
        driver.quit()
    
    # Final summary
    print("\n" + "="*70)
    print("📊 SCRAPING SUMMARY")
    print("="*70)
    
    summary_df = pd.DataFrame(results)
    print(summary_df.to_string(index=False))
    
    successful = len([r for r in results if r['Status'] == "✅ Success"])
    failed = len([r for r in results if '❌' in r['Status']])
    
    print(f"\n{'='*70}")
    print(f"✅ Successful: {successful}/{len(companies_df)}")
    print(f"❌ Failed: {failed}/{len(companies_df)}")
    print(f"📁 Output location: {output_dir.absolute()}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
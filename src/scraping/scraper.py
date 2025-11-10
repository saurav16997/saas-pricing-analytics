from selenium import webdriver 
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.chrome.options import Options 
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager 
import pandas as pd 
from pathlib import Path 
import time 
import re

def scrape_notion_pricing():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    print("🚀 Launching Chrome browser...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.maximize_window()

    url = "https://www.notion.so/pricing"
    print(f"📍 Navigating to {url} ...")
    driver.get(url)
    
    print("⏳ Waiting for page to load...")
    time.sleep(8)
    
    for i in range(3):
        driver.execute_script(f"window.scrollTo(0, {(i+1) * 500});")
        time.sleep(1)
    
    time.sleep(3)
    
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    
    pricing_data = []
    tier_names = ["Free", "Plus", "Business", "Enterprise"]
    
    print("\n🔍 Extracting pricing data...")
    
    for tier in tier_names:
        print(f"\n--- Processing {tier} plan ---")
        try:
            # Find tier heading
            tier_elements = driver.find_elements(By.XPATH, f"//*[text()='{tier}']")
            
            tier_heading = None
            for elem in tier_elements:
                if len(elem.text.strip()) < 30:
                    tier_heading = elem
                    break
            
            if not tier_heading:
                print(f"❌ Could not find {tier} heading")
                continue
            
            print(f"✅ Found {tier} heading")
            
            # Navigate up to find container - try multiple strategies
            current = tier_heading
            containers_by_size = []
            
            # Go up 8 levels and collect all candidates
            for level in range(8):
                current = driver.execute_script("return arguments[0].parentElement;", current)
                
                try:
                    lis = current.find_elements(By.TAG_NAME, "li")
                    li_count = len(lis)
                    
                    if li_count > 0:
                        # Check if other tiers mentioned
                        text = current.text
                        other_tiers = [t for t in tier_names if t != tier]
                        other_count = sum(1 for t in other_tiers if t in text)
                        
                        containers_by_size.append({
                            'element': current,
                            'li_count': li_count,
                            'level': level,
                            'other_tiers': other_count
                        })
                except:
                    pass
            
            # Pick best container: prefer ones with 5-35 items and no other tiers
            best = None
            for c in containers_by_size:
                if 5 <= c['li_count'] <= 35 and c['other_tiers'] == 0:
                    best = c
                    break
            
            # Fallback: smallest with features
            if not best and containers_by_size:
                best = min(containers_by_size, key=lambda x: x['li_count'])
            
            if not best:
                print(f"   ⚠️ No suitable container found")
                continue
            
            container = best['element']
            print(f"   Using container: level {best['level']}, {best['li_count']} items, {best['other_tiers']} other tiers")
            
            # Extract price
            price_text = container.text.lower()
            
            if 'contact sales' in price_text or 'contact us' in price_text:
                price = "Contact sales"
            elif re.search(r'\$\d+', container.text):
                match = re.search(r'\$\d+', container.text)
                price = match.group(0)
            elif tier == "Free" or 'free' in price_text:
                price = "Free"
            elif tier == "Enterprise":
                price = "Contact sales"
            else:
                price = "N/A"
            
            print(f"   Price: {price}")
            
            # Extract features
            features = []
            try:
                lis = container.find_elements(By.TAG_NAME, "li")
                for li in lis[:20]:
                    feat = li.text.strip()
                    if feat and len(feat) > 5 and feat not in features:
                        features.append(feat)
            except:
                pass
            
            print(f"   Features: {len(features)} extracted")
            
            pricing_data.append({
                "Tier": tier,
                "Price": price,
                "Features": " | ".join(features) if features else "N/A"
            })
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            continue
    
    if not pricing_data:
        print("\n❌ No data extracted!")
        driver.save_screenshot("data/raw/notion_screenshot.png")
        driver.quit()
        return
    
    df = pd.DataFrame(pricing_data)
    output_path = Path("data/raw/notion_pricing.csv")
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ Saved to {output_path}")
    print("\n📊 Results:")
    for _, row in df.iterrows():
        feats = row['Features'].split(' | ')
        print(f"\n{row['Tier']}: {row['Price']}")
        for i, f in enumerate(feats[:3], 1):
            print(f"  {i}. {f[:70]}")
        if len(feats) > 3:
            print(f"  ... +{len(feats)-3} more")
    
    time.sleep(3)
    driver.quit()

if __name__ == "__main__":
    print("="*60)
    print("Notion Pricing Scraper v6.0")
    print("="*60)
    scrape_notion_pricing()
    print("\n" + "="*60)
    print("Done!")
    print("="*60)
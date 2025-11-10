import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

# Create output directory if not exists
Path("data/raw").mkdir(parents=True, exist_ok=True)

# URL to scrape
url = "https://www.notion.so/pricing"

# Step 1: Fetch HTML content
print(f"Fetching pricing data from {url} ...")
response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
soup = BeautifulSoup(response.text, "html.parser")

# Step 2: Inspect the structure manually (for debugging)
# We'll extract tier names and prices
tiers, prices, features = [], [], []

for card in soup.find_all("div", class_="pricingTier"):
    tier_name = card.find("h2")
    price = card.find("span", class_="price")
    feature_list = card.find_all("li")

    tiers.append(tier_name.text.strip() if tier_name else None)
    prices.append(price.text.strip() if price else None)
    features.append(", ".join([li.text.strip() for li in feature_list]))

# Step 3: Build a DataFrame
df = pd.DataFrame({
    "Tier": tiers,
    "Price": prices,
    "Features": features
})

# Step 4: Save it
output_path = Path("data/raw/notion_pricing.csv")
df.to_csv(output_path, index=False)
print(f"✅ Saved pricing data to {output_path}")

print(df.head())

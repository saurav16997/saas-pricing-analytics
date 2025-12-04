import pandas as pd

print("=== RAW ASANA FILE ===")
df = pd.read_csv("data/raw/asana_pricing.csv")
print(df[['Tier', 'Price']])

print("\n=== STAGING FILE ===")
df2 = pd.read_csv("data/staging/all_pricing_combined.csv")
print(df2[df2['Company'] == 'Asana'][['Company', 'Tier', 'Price']])

print("\n=== CLEAN FILE ===")
df3 = pd.read_csv("data/clean/pricing_standardized.csv")
print(df3[df3['Company'] == 'Asana'][['Company', 'Tier', 'Price_Original', 'Price_Numeric']])
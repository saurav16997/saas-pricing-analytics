🚀 SaaS Pricing Intelligence Dashboard

A full-stack competitive intelligence tool analyzing pricing strategies across 9 major SaaS companies (Notion, Asana, ClickUp, etc.).

🎯 Project Goal

To reverse-engineer the pricing models of top B2B SaaS players and answer the critical business question: "How do pricing levers like SSO, seat counts, and tier differentiation actually impact revenue and retention?"

💡 Key Strategic Insights

1. 🔒 The "SSO Tax" is Real (Major Finding)

One of the most significant findings from this analysis is the premium placed on security.

The Data: Plans including Single Sign-On (SSO) cost an average of $34.70/user/month, compared to $17.85 for standard plans.

The Insight: Companies use security features as a "gatekeeper" to force mid-sized companies into Enterprise tiers, effectively doubling the Average Revenue Per User (ARPU).

Recommendation: For B2B SaaS, keeping SSO gated is a massive lever for ARPU expansion.

2. 📉 The Retail Industry "Churn Trap"

Observation: The Retail segment showed high initial willingness to pay (High ARPU) but suffered from a churn rate >30%.

Strategic Shift: In contrast, Technology and Consulting sectors showed much stronger retention.

Action: Marketing spend should be diverted away from Retail and focused on high-retention sectors to improve Customer Lifetime Value (LTV).

3. 🏆 Market Positioning Matrix

The "Whale Hunter": ClickUp targets high-value users with an ARPU >$90, focusing on extracting maximum value from fewer customers.

The "Net Caster": Trello plays the volume game (Product-Led Growth), capturing massive user counts at a lower price point (~$4 ARPU).

🛠 Technical Architecture

This project simulates a complete Data Engineering & Analytics lifecycle:

Stage

Technology

Description

1. Collection

Python (BeautifulSoup)

Built a custom web scraper to extract pricing tiers, feature lists, and limits from competitor websites.

2. Engineering

SQLite & SQL

Designed a normalized schema (saas_analytics.db) to store unstructured web data.

3. Simulation

Python (Faker)

Generated 100,000 synthetic records (users, subscriptions, events) to model realistic churn and revenue scenarios based on the scraped pricing rules.

4. Analysis

Pandas & SQL

Calculated advanced metrics: ARPU, ARR, Churn Rate, and Feature Penetration.

5. Visualization

Streamlit

Built an interactive "North Star" dashboard for executive decision-making.

📸 Dashboard Previews

The Executive "North Star" View

A high-level health check showing real-time Market Size, Total Revenue, and Churn Risk.
![leaderboard](https://github.com/user-attachments/assets/1addd33d-3a7d-4cdf-8f46-39e7664fcc7d)


Strategic Segmentation

Visualizing the trade-off between Revenue (ARPU) and Risk (Churn) across different industries.

![pricing](https://github.com/user-attachments/assets/0eb6b2d2-ae30-456b-9e0b-dfa749e684d3)
![industry_strategy](https://github.com/user-attachments/assets/98bb8257-542a-4f29-9e78-916dddb84681)


⚙️ How to Run Locally

1. Clone the Repository

git clone [https://github.com/saurav16997/saas-pricing-analytics.git](https://github.com/saurav16997/saas-pricing-analytics.git)
cd saas-pricing-analytics


2. Install Dependencies

pip install -r requirements.txt


3. Launch the Dashboard

streamlit run src/dashboard/app.py


4. (Optional) Run Analytical Scripts

To re-process the data and generate new CSV reports:

python src/analysis/run_comparative_analysis.py


📂 Project Structure

├── data/
│   ├── saas_analytics.db    # The SQLite database (excluded from git)
│   └── analysis/            # Generated CSV reports
├── src/
│   ├── extract/             # Web scrapers
│   ├── etl/                 # Data cleaning pipelines
│   ├── analysis/            # SQL queries & Python scripts
│   └── dashboard/           # Streamlit app code
└── README.md

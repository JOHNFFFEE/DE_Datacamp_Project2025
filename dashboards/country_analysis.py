import plotly.express as px
import pandas as pd
from google.cloud import bigquery
from prefect import get_run_logger
from prefect_gcp import GcpCredentials

import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))  # Adjust based on your structure




gcp_credentials_block = GcpCredentials.load("gcs")

# Load GCP credentials
try:
    gcp_credentials_block = GcpCredentials.load("gcs")
except Exception as e:
    logger = get_run_logger()
    logger.error(f"Failed to load GCP credentials: {str(e)}")
    raise

    # Get credentials and initialize client
credentials = gcp_credentials_block.get_credentials_from_service_account()
client = bigquery.Client(
    credentials=credentials,
    project="acoustic-env-454618-v3"
)

# Query to fetch your data
query = """
SELECT 
    name as country, 
    vacc_year as year, 
    max(percentage_vaccinated) as percentage_vaccinated 
FROM `acoustic-env-454618-v3.dbt_jdarevill.fact_population_vaccinated`
WHERE name IS NOT NULL  
GROUP BY name, vacc_year
ORDER BY percentage_vaccinated DESC
"""

# Execute query and load results
df = client.query(query).to_dataframe()

# DATA CLEANING
# 1. Remove extreme outliers (values > 1000%)
cleaned_df = df[df['percentage_vaccinated'] <= 1000].copy()

# 2. Format percentages properly (assuming original values should be 0-100%)
cleaned_df['percentage'] = cleaned_df['percentage_vaccinated'].clip(0, 100)

# 3. Filter recent years if needed
cleaned_df = cleaned_df[cleaned_df['year'] >= 2020]

# Create visualization
fig = px.bar(cleaned_df.sort_values('percentage', ascending=False),
             x="country",
             y="percentage",
             color="percentage",
             color_continuous_scale='Bluered',
             title="Vaccination Rates by Country (0-100% scale)",
             labels={'percentage': 'Vaccination %'},
             height=600)

# Customize layout
fig.update_layout(
    xaxis={'categoryorder':'total descending'},
    yaxis_range=[0, 100],  # Force 0-100% scale
    hovermode="x unified"
)

# Add reference lines
fig.add_hline(y=50, line_dash="dot", 
             annotation_text="50% Reference", 
             annotation_position="top right")

fig.show()
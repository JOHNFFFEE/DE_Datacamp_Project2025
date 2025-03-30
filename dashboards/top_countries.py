import plotly.express as px
import pandas as pd
from google.cloud import bigquery
from prefect import get_run_logger
from prefect_gcp import GcpCredentials


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

# Query data
query = """
SELECT 
    vacc_year as Year, 
    Country_name as Country, 
    people_vaccinated/1000000 as people_vaccinated_millions  -- Convert to millions
FROM `acoustic-env-454618-v3.dbt_jdarevill.fact_ten_top_vaccination_countries`
WHERE Country_name IS NOT NULL
ORDER BY Year ASC, people_vaccinated DESC
"""

df = client.query(query).to_dataframe()

# Create visualization
fig = px.bar(df,
            x="Country",
            y="people_vaccinated_millions",
            color="Year",
            barmode="group",
            title="Top Countries Vaccination Numbers by Year (in Millions)",
            labels={
                "people_vaccinated_millions": "People Vaccinated (Millions)",
                "Country": "Country"
            },
            height=600,
            text_auto='.2s')  # Auto-format numbers

# Customize layout
fig.update_layout(
    yaxis_title="People Vaccinated (Millions)",
    xaxis_title="",
    plot_bgcolor='rgba(0,0,0,0)',
    hovermode="x unified",
    xaxis={'categoryorder':'total descending'}
)

# Format tooltips
fig.update_traces(
    hovertemplate="<b>%{x} (%{customdata})</b><br>%{y:.1f}M vaccinated<extra></extra>",
    customdata=df["Year"],
    textposition='outside'
)

# Add reference line for average
avg = df.groupby('Country')['people_vaccinated_millions'].mean()
for country, value in avg.items():
    fig.add_hline(y=value, line_dash="dot", 
                 annotation_text=f"{country} avg: {value:.1f}M", 
                 annotation_position="top right",
                 line_width=1)

fig.show()
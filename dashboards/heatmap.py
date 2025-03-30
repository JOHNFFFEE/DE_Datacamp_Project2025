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

query = """
SELECT 
    name as country, 
    vacc_year as year, 
    max(percentage_vaccinated) as vaccination_percentage
FROM `acoustic-env-454618-v3.dbt_jdarevill.fact_population_vaccinated`
WHERE name IS NOT NULL
GROUP BY name, vacc_year
ORDER BY year, vaccination_percentage DESC
"""

df = client.query(query).to_dataframe()

# Create animated heatmap with percentages
fig = px.choropleth(df,
                   locations="country",
                   locationmode="country names",
                   color="vaccination_percentage",
                   hover_name="country",
                   animation_frame="year",
                   color_continuous_scale=px.colors.sequential.Viridis,
                   range_color=(0, 100),  # Fixed 0-100% scale
                   title="Global Vaccination Percentage by Country and Year",
                   height=600,
                   labels={'vaccination_percentage': 'Vaccination %'})

# Customize layout
fig.update_layout(
    geo=dict(
        showframe=False,
        showcoastlines=True,
        projection_type='natural earth'
    ),
    coloraxis_colorbar=dict(
        title="% Vaccinated",
        thickness=20,
        len=0.75,
        ticksuffix="%"
    ),
    margin=dict(l=0, r=0, t=50, b=0)
)

# Enhance animation
fig.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 1000
fig.layout.updatemenus[0].buttons[0].args[1]['transition']['duration'] = 500

# Add reference annotations
fig.add_annotation(
    text="Data shows percentage of population vaccinated",
    xref="paper", yref="paper",
    x=0.01, y=0.01, showarrow=False,
    font=dict(size=10)
)

# Highlight threshold values
for threshold in [30, 50, 70]:
    fig.add_annotation(
        x=0.5, y=0.05,
        xref='paper', yref='paper',
        text=f"{threshold}% Vaccination Target",
        showarrow=False,
        font=dict(color='white' if threshold > 50 else 'black')
    )

fig.show()
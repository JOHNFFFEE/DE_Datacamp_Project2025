from prefect import flow, task
from prefect_gcp import GcpCredentials
from pathlib import Path
import os

@task
def enforce_structure():
    """Guarantee correct file structure exists"""
    dbt_path = Path("dbt").absolute()
    dbt_path.mkdir(exist_ok=True)
    
    # Create dbt_project.yml if missing
    project_file = dbt_path / "dbt_project.yml"
    if not project_file.exists():
        project_file.write_text(f"""\
name: 'covid_vaccination'
version: '1.0.0'
config-version: 2
profile: 'default'
model-paths: ["models"]
""")
    
    # Verify models directory
    models_dir = dbt_path / "models"
    models_dir.mkdir(exist_ok=True)
    if not list(models_dir.glob("*.sql")):
        (models_dir / "stg_covid.sql").write_text("""\
{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'vaccinations') }}
""")

@flow
def dbt_pipeline():
    enforce_structure()
    
    # Set ABSOLUTE paths
    dbt_dir = Path("dbt").absolute()
    os.environ["GCP_KEYFILE"] = str(Path("config/service_account.json").absolute())
    os.environ["DBT_PROFILES_DIR"] = str(dbt_dir)
    
    # Force working directory to project root
    os.chdir(Path(__file__).parent)
    
    # Debug output
    print(f"Current directory: {os.getcwd()}")
    print(f"dbt_project.yml exists: {(dbt_dir/'dbt_project.yml').exists()}")
    
    # Execute dbt
    os.system(f"cd {dbt_dir} && dbt debug && dbt build")

if __name__ == "__main__":
    dbt_pipeline()
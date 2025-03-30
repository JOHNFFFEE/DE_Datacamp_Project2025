{{
    config(
        materialized='table'
    )
}}


WITH vaccination_data_aggregated AS (
    SELECT 
        -- Generate a primary key for the fact table
        ROW_NUMBER() OVER (ORDER BY v.vaccination_date) AS vaccination_fact_id,

        -- Country and Date foreign keys
       c.Name,
       v.vacc_year,  -- Extract year for aggregation

        -- Aggregated vaccination data measures
        SUM(v.total_vaccinations) AS total_vaccinations,
        SUM(v.people_vaccinated) AS people_vaccinated,
        SUM(v.people_fully_vaccinated) AS people_fully_vaccinated,
        SUM(v.total_boosters) AS total_boosters,
        SUM(v.daily_vaccinations) AS daily_vaccinations,
        SUM(v.daily_vaccinations_raw) AS daily_vaccinations_raw,
        SUM(v.total_vaccinations_per_hundred) AS total_vaccinations_per_hundred,
        SUM(v.people_vaccinated_per_hundred) AS people_vaccinated_per_hundred,
        SUM(v.people_fully_vaccinated_per_hundred) AS people_fully_vaccinated_per_hundred,
        SUM(v.total_boosters_per_hundred) AS total_boosters_per_hundred,
        SUM(v.daily_vaccinations_per_million) AS daily_vaccinations_per_million,
        SUM(v.daily_people_vaccinated) AS daily_people_vaccinated,
        SUM(v.daily_people_vaccinated_per_hundred) AS daily_people_vaccinated_per_hundred

    FROM {{ ref('stg_staging__vaccination_data_optimized') }} v
    LEFT JOIN {{ ref('dim_country_code') }} c
        ON v.iso_code = c.iso_code  -- Join on country code
    GROUP BY
        v.vaccination_date,
        c.Name,
        v.vacc_year -- Group by country and year
),

vaccinated_vs_population AS (
    SELECT 
        va.vaccination_fact_id,
        va.Name,
        va.vacc_year,
        va.people_vaccinated,
        -- va.total_vaccinations,
        -- va.people_fully_vaccinated,
        -- va.total_boosters,
        -- va.daily_vaccinations,
        -- va.daily_vaccinations_raw,
        -- va.total_vaccinations_per_hundred,
        -- va.people_vaccinated_per_hundred,
        -- va.people_fully_vaccinated_per_hundred,
        -- va.total_boosters_per_hundred,
        -- va.daily_vaccinations_per_million,
        -- va.daily_people_vaccinated,
        -- va.daily_people_vaccinated_per_hundred,

        -- Lookup the total population for the country from the country_lookup CSV or table
        po.total_population,
        
        -- Calculate the percentage of people vaccinated for this country and year
        (va.people_vaccinated / po.total_population) * 100 AS percentage_vaccinated

    FROM vaccination_data_aggregated va
    LEFT JOIN {{ ref('dim_population') }} po
        ON va.Name = po.Name  -- Join on country_id to get population data
)

SELECT *
FROM vaccinated_vs_population

WITH vaccination_data_aggregated AS (
    SELECT 
        -- Generate a primary key for the fact table
        ROW_NUMBER() OVER (ORDER BY v.vacc_year, v.location) AS vaccination_fact_id,

        -- Country and Date foreign keys
        v.location,
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
       v.location,
        v.vacc_year 
          -- Group by country and year
)

SELECT * 
FROM vaccination_data_aggregated
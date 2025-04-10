with min_finder as (
  SELECT 
    date,
    country_code,
    MIN(subregion1_code) as subregion1_code ,
    MIN(subregion2_code) as subregion2_code,
  FROM
    `bigquery-public-data.covid19_open_data.covid19_open_data`
  GROUP BY 1, 2),
lal as (
  SELECT
    date,
    country_code,
    CASE
      WHEN subregion2_code is not null
      THEN 2
      WHEN subregion1_code is not null
      THEN 1
      ELSE 0
    END as leaf_aggregation_level
  FROM
    min_finder)
SELECT
  covid.subregion1_name AS province_state,
  covid.country_name AS country_region,
  covid.date,
  latitude, 
  longitude, 
  subregion1_name AS sub_region1_name,
  location_geometry AS location_geom,
  cumulative_confirmed AS confirmed,
  cumulative_deceased AS deaths,
  cumulative_recovered AS recovered,
  CASE
    WHEN cumulative_confirmed is NULL THEN NULL
    WHEN cumulative_deceased is NULL THEN NULL
    WHEN cumulative_recovered is NULL THEN NULL
    ELSE (cumulative_confirmed-cumulative_recovered-cumulative_deceased)
    END
  AS active,
  subregion2_code AS fips,
  subregion2_name AS admin_2,
  location_key AS combined_key
FROM
  `bigquery-public-data.covid19_open_data.covid19_open_data` covid
  JOIN lal on lal.date = covid.date
  AND lal.country_code = covid.country_code
  AND lal.leaf_aggregation_level = covid.aggregation_level
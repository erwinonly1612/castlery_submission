{{ config(materialized = 'table') }}

SELECT
LEFT(RIGHT(url,2),1) AS id, 
title, 
episode_id, 
opening_crawl, 
COUNT(DISTINCT characters) AS character_count,
COUNT(DISTINCT planets) AS planet_count,
COUNT(DISTINCT starships) AS starship_count,
COUNT(DISTINCT vehicles) AS vehicle_count,
COUNT(DISTINCT species) AS species_count,
release_date,
created,
edited

FROM {{ source('staging', 'staging_films') }}
LEFT JOIN
unnest(json_extract_array(`characters`)) characters
LEFT JOIN
unnest(json_extract_array(`planets`)) planets
LEFT JOIN
unnest(json_extract_array(`starships`)) starships
LEFT JOIN
unnest(json_extract_array(`vehicles`)) vehicles
LEFT JOIN
unnest(json_extract_array(`species`)) species

GROUP BY 
LEFT(RIGHT(url,2),1), 
title, 
episode_id, 
opening_crawl,
release_date,
created,
edited
{{ config(materialized = 'table') }}

SELECT 
LEFT(RIGHT(people_stg_table.url,2),1) AS id, 
name, 
height, 
mass, 
hair_color,
skin_color,
eye_color,
birth_year,
gender,
planets_stg_table.string_content AS homeworld,
COUNT(DISTINCT species) AS species_count,
COUNT(DISTINCT vehicles) AS vehicle_count,
COUNT(DISTINCT starships) AS starship_count,
created,
edited

FROM {{ source('staging', 'staging_people') }} people_stg_table
LEFT JOIN {{ source('staging', 'staging_planets') }} planets_stg_table
ON people_stg_table.`homeworld` = planets_stg_table.`url`
LEFT JOIN unnest(json_extract_array(`species`)) species
LEFT JOIN unnest(json_extract_array(`vehicles`)) vehicles
LEFT JOIN unnest(json_extract_array(`starships`)) starships

GROUP BY 
LEFT(RIGHT(people_stg_table.url,2),1), 
name,
height, 
mass, 
hair_color,
skin_color,
eye_color,
birth_year,
gender,
planets_stg_table.string_content,
created,
edited

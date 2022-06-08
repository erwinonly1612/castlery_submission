{{ config(materialized = 'table') }}
SELECT 
LEFT(RIGHT(url,2),1) AS film_id, 
LEFT(RIGHT(characters,3),1) as people_id
FROM {{ source('staging', 'staging_films') }}
LEFT JOIN
unnest(json_extract_array(`characters`)) characters
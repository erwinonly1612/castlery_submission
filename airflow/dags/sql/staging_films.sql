INSERT {{ BIGQUERY_DATASET }}.{{ STAGING_FILMS_TABLE }}
SELECT 
    COALESCE(title, 'NA') AS title,
    CAST(episode_id AS INTEGER) AS episode_id,
    COALESCE(opening_crawl, 'NA') AS opening_crawl,
    COALESCE(director, 'NA') AS director,
    COALESCE(producer, 'NA') AS producer,
    CAST(release_date AS DATE) AS release_date,
    COALESCE(species, 'NA') AS species,
    COALESCE(starships, 'NA') AS starships,
    COALESCE(vehicles, 'NA') AS vehicles,
    COALESCE(characters, 'NA') AS characters,
    COALESCE(planets, 'NA') AS planets,
    COALESCE(url, 'NA') AS VARCHAR,
    CAST(created AS DATETIME) AS created,    
    CAST(edited AS DATETIME) AS edited
FROM {{ BIGQUERY_DATASET }}.{{ STAGING_FILMS_EXTERNAL_TABLE }}
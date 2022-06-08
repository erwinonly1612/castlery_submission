INSERT {{ BIGQUERY_DATASET }}.{{ FACT_TOP_MOVIES_TABLE }}
SELECT 
    COALESCE(year, 0) AS year,
    COALESCE(title, 'NA') AS title,
    COALESCE(reviews, 0) AS reviews,
    CAST(tomatometer AS DECIMAL) AS tomatometer,
    CAST(audience_score AS DECIMAL) AS audience_score,
    COALESCE(synopsis, 'NA') AS synopsis,
    COALESCE(rating, 'NA') AS rating,
    COALESCE(genre, 'NA') AS genre,
    COALESCE(original_language, 'NA') AS original_language,
    COALESCE(director, 'NA') AS director,
    COALESCE(producer, 'NA') AS producer,
    COALESCE(writer, 'NA') AS writer,
    CAST(release_date_theaters AS DATETIME) AS release_date_theaters,
    CAST(release_date_streaming AS DATETIME) AS release_date_streaming,
    runtime_mins,
    COALESCE(distributor, 'NA') AS distributor,
    COALESCE(production_co, 'NA') AS production_co,
    COALESCE(aspect_ratio, 'NA') AS aspect_ratio,
    COALESCE(url, 'NA') AS url
FROM {{ BIGQUERY_DATASET }}.{{ FACT_TOP_MOVIES_EXTERNAL_TABLE }}
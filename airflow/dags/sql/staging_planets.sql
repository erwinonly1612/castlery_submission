INSERT {{ BIGQUERY_DATASET }}.{{ STAGING_PLANETS_TABLE }}
SELECT 
    url,
    string_content
FROM {{ BIGQUERY_DATASET }}.{{ STAGING_PLANETS_EXTERNAL_TABLE }}

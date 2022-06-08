INSERT {{ BIGQUERY_DATASET }}.{{ STAGING_PEOPLE_TABLE }}
SELECT 
    COALESCE(name, 'NA') AS name,
    COALESCE(birth_year , 'NA') AS birth_year,
    COALESCE(eye_color, 'NA') AS eye_color,
    COALESCE(gender, 'NA') AS gender,
    COALESCE(hair_color, 'NA') AS hair_color,
    CAST(height AS INTEGER) AS height,
    CAST(mass AS INTEGER) AS mass,
    COALESCE(skin_color, 'NA') AS skin_color,
    COALESCE(homeworld, 'NA') AS homeworld,
    films AS films,
    species AS species,    
    starships AS starships,
    vehicles AS vehicles,
    COALESCE(url, 'NA') AS VARCHAR,    
    CAST(created AS DATETIME) AS created,    
    CAST(edited AS DATETIME) AS edited
FROM {{ BIGQUERY_DATASET }}.{{ STAGING_PEOPLE_EXTERNAL_TABLE }}

schema = {
    "staging_films" : [
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "episode_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "opening_crawl", "type": "STRING", "mode": "NULLABLE"},
        {"name": "director", "type": "STRING", "mode": "NULLABLE"},
        {"name": "producer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "release_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "species", "type": "STRING", "mode": "NULLABLE"},
        {"name": "starships", "type": "STRING", "mode": "NULLABLE"},
        {"name": "vehicles", "type": "STRING", "mode": "NULLABLE"},
        {"name": "characters", "type": "STRING", "mode": "NULLABLE"},
        {"name": "planets", "type": "STRING", "mode": "NULLABLE"},
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "edited", "type": "DATETIME", "mode": "NULLABLE"}
    ],
    "staging_people" : [
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "birth_year", "type": "STRING", "mode": "NULLABLE"},        
        {"name": "eye_color", "type": "STRING", "mode": "NULLABLE"},
        {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "hair_color", "type": "STRING", "mode": "NULLABLE"},
        {"name": "height", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "mass", "type": "INTEGER", "mode": "NULLABLE"},        
        {"name": "skin_color", "type": "STRING", "mode": "NULLABLE"},
        {"name": "homeworld", "type": "STRING", "mode": "NULLABLE"},
        {"name": "films", "type": "STRING", "mode": "NULLABLE"},
        {"name": "species", "type": "STRING", "mode": "NULLABLE"},
        {"name": "starships", "type": "STRING", "mode": "NULLABLE"},  
        {"name": "vehicles", "type": "STRING", "mode": "NULLABLE"},
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created", "type": "DATETIME", "mode": "NULLABLE"},
        {"name": "edited", "type": "DATETIME", "mode": "NULLABLE"}
    ],
    "staging_planets" : [
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "string_content", "type": "STRING", "mode": "NULLABLE"}
    ]
}
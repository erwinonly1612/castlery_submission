import configparser

# CONFIG
# Use configparser to read in the variables to connect with Amazon Redshift
# create a IAM user first, fill in dwh.cfg file

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_film_table_drop = "DROP TABLE IF EXISTS staging_film"
staging_people_table_drop = "DROP TABLE IF EXISTS staging_people"
dim_film_table_drop = "DROP TABLE IF EXISTS dim_film"
dim_people_table_drop = "DROP TABLE IF EXISTS dim_people"
film_people_map_table_drop = "DROP TABLE IF EXISTS film_people_map"

# CREATE TABLES
# staging tables for reading data on s3
staging_film_table_create= ("""
    CREATE TABLE staging_film (
        title                  VARCHAR,
        episode_id             INTEGER,
        opening_crawl          VARCHAR,
        director               VARCHAR,
        producer               VARCHAR,
        release_date           DATE,
        characters             VARCHAR,
        planets                VARCHAR,
        starships              VARCHAR,
        vehicles               VARCHAR,
        species                VARCHAR,
        created                DATETIME,
        edited                 DATETIME,
        url                    VARCHAR      
    )    
""")

staging_people_table_create = ("""
    CREATE TABLE staging_people (
        name                   VARCHAR,
        height                 INTEGER,
        mass                   INTEGER,
        hair_color             VARCHAR,
        skin_color             VARCHAR,
        eye_color              VARCHAR,
        birth_year             VARCHAR,
        gender                 VARCHAR,
        homeworld              VARCHAR,
        films                  VARCHAR,
        species                VARCHAR,
        vehicles               VARCHAR,
        starships              VARCHAR,
        created                DATETIME,
        edited                 DATETIME,
        url                    VARCHAR
    )
""")

# fact and dimension tables including songplays, songs, users, artists, times.
dim_film_table_create = ("""
    CREATE TABLE dim_film (
        id                     INTEGER primary key, 
        title                  VARCHAR,
        episode_id             INTEGER,
        opening_crawl          VARCHAR,
        director               VARCHAR,
        producer               VARCHAR,        
        character_count        INTEGER,
        planet_count           INTEGER,
        starship_count         INTEGER,
        vehicle_count          INTEGER,
        species_count          INTEGER,
        release_date           DATE,
        created                DATETIME,
        edited                 DATETIME
    )
""")

dim_people_table_create = ("""
    CREATE TABLE dim_people(
        id                     INTEGER primary key,
        name                   VARCHAR,
        height                 INTEGER,
        mass                   INTEGER,
        hair_color             VARCHAR,
        skin_color             VARCHAR,
        eye_color              VARCHAR,
        birth_year             VARCHAR,
        gender                 VARCHAR,
        homeworld              VARCHAR,
        films                  VARCHAR,
        species_count          INTEGER,
        vehicle_count          INTEGER,
        starship_count         INTEGER,
        created                DATETIME,
        edited                 DATETIME
    )
""")

film_people_map_table_create = ("""
    CREATE TABLE film_people_map (
        film_id                INTEGER PRIMARY KEY,
        people_id              INTEGER PRIMARY KEY
    )
""")

# STAGING TABLES
# let COPY automatically load fields from the JSON file by specifying the 'auto' option, or you can specify a JSONPaths file that COPY uses to parse the JSON source data. 
# When moving large amounts of data from S3 staging area to Redshift, it is better to use the copy command instead of insert. The benefit of using the copy command is that the ingestion can be parallelized if the data is broken integero parts. Each part can be independently ingested by a slice in the cluster. 

staging_events_copy = ("""
    COPY staging_events FROM {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format      as JSON {path}
    timeformat  as 'epochmillisecs'
""").format(bucket=LOG_DATA, role=IAM_ROLE, path=LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format      as JSON 'auto'
    timeformat  as 'epochmillisecs'
""").format(bucket=SONG_DATA, role=IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        e.ts          as start_time, 
        e.userId      as user_id, 
        e.level       as level,
        s.song_id      as song_id,
        s.artist_id    as artist_id,
        e.sessionId   as session_id,
        e.location    as location,
        e.userAgent   as user_agent
    FROM staging_events e join staging_songs s
    ON e.song = s.title
    AND e.artist = s.artist_name
    AND e.page = 'NextSong'
    AND e.length = s.duration
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        e.userId       as user_id,
        e.firstName    as first_name,
        e.lastName     as last_name,
        e.gender       as gender,
        e.level        as level
    FROM staging_events e
    WHERE e.userId IS NOT NULL
    AND page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        s.song_id       as song_id,
        s.title        as title,
        s.artist_id     as artist_id,
        s.year         as year,
        s.duration     as duration
    FROM staging_songs s
    WHERE s.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        s.artist_id         as artist_id,
        s.artist_name       as name,
        s.artist_location   as location,
        s.artist_latitude   as latitude,
        s.artist_longitude  as longitude
    FROM staging_songs s
    WHERE s.artist_id IS NOT NULL    
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time                       as start_time,
        extract(hour from start_time)    as hour,
        extract(day from start_time)     as day,
        extract(week from start_time)    as week,
        extract(month from start_time)   as month,
        extract(year from start_time)    as year,
        extract(weekday from start_time) as weekday
    FROM songplays
""")

# for checking if the tables are created successfully
# count the number of rows
staging_events_count = ("""
    select count(*) from staging_events
""")

staging_songs_count = ("""
    select count(*) from staging_songs
""")

songplay_count = ("""
    select count(*) from songplays
""")

user_count = ("""
    select count(*) from users
""")

song_count = ("""
    select count(*) from songs
""")

artist_count = ("""
    select count(*) from artists
""")

time_count = ("""
    select count(*) from times
""")

# QUERY LISTS
# create_table_queries and drop_table_queries for create_tables.py
# copy_table_queries and insert_table_queries for eti.py
# count_rows_queries for analytics.py

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_rows_queries = [staging_events_count, staging_songs_count, songplay_count, user_count, song_count, artist_count, time_count]

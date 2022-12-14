# DROP TABLES

songplay_table_drop = "Drop table IF EXISTS songplays"
user_table_drop = "Drop table IF EXISTS users"
song_table_drop = "Drop table IF EXISTS songs"
artist_table_drop = "Drop table IF EXISTS artists"
time_table_drop = "Drop table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id   SERIAL PRIMARY KEY,
  start_time    TIMESTAMP NOT NULL, 
  user_id       INT NOT NULL,
  level         VARCHAR, 
  song_id       VARCHAR,
  artist_id     VARCHAR, 
  session_id    INT,
  location      VARCHAR, 
  user_agent    VARCHAR
  )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
  user_id     INT PRIMARY KEY,
  first_name  VARCHAR,
  last_name   VARCHAR,
  gender      VARCHAR,
  level       VARCHAR
  )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
  song_id    VARCHAR PRIMARY KEY,
  title      VARCHAR NOT NULL,
  artist_id  VARCHAR,
  year       INT,
  duration   NUMERIC NOT NULL
  )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id    VARCHAR PRIMARY KEY,
  name         VARCHAR NOT NULL,
  location     VARCHAR,
  latitude     FLOAT,
  longitude    FLOAT
  )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
  start_time   TIMESTAMP PRIMARY KEY,
  hour         INT,
  day          INT,
  week         INT,
  month        INT,
  year         INT,
  weekday      VARCHAR
  )
""")



# INSERT RECORDS

user_table_insert = ("""
INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(user_id)
DO UPDATE 
    SET level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(song_id) 
DO UPDATE
    SET title = excluded.title,
        artist_id = excluded.artist_id,
        year = excluded.year,
        duration = excluded.duration;
                    
""")

artist_table_insert = ("""
INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(artist_id) DO UPDATE
    SET name = excluded.name,
        location = excluded.location,
        latitude = excluded.latitude,
        longitude = excluded.longitude;
""")


time_table_insert = ("""
INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(start_time)DO NOTHING;
""")


songplay_table_insert = ("""
INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent) 
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(songplay_id) DO NOTHING;;
""")

# FIND SONGS

song_select = ("""
SELECT
        s.song_id, 
        s.artist_id 
FROM   
        songs s, artists a 
WHERE  
        a.artist_id = s.artist_id 
        AND s.title =%s 
        AND a.name =%s 
        AND s.duration =%s;""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, songplay_table_create,time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
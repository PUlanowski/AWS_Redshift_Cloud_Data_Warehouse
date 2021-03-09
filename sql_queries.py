import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ('''CREATE TABLE IF NOT EXISTS staging_events(
artist VARCHAR,
auth VARCHAR,
first_name VARCHAR,
gender VARCHAR,
item_in_session INTEGER,
last_name VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
session_id INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
user_agent VARCHAR,
user_id INTEGER
);''')

staging_songs_table_create = ('''CREATE TABLE IF NOT EXISTS staging_songs(
artist_id VARCHAR,
artist_latitude FLOAT,
artist_location VARCHAR,
artist_longitude FLOAT,
artist_name VARCHAR,
duration FLOAT,
num_songs INTEGER,
song_id VARCHAR,
title VARCHAR,
year INTEGER);''')

songplay_table_create = ('''CREATE TABLE IF NOT EXISTS songplays(
                songplay_id integer identity(0,1) CONSTRAINT pk_songplays PRIMARY KEY,
                start_time timestamp UNIQUE NOT NULL,
                user_id varchar,
                level varchar NOT NULL,
                song_id varchar,
                artist_id varchar,
                session_id integer NOT NULL,
                location varchar,
                user_agent varchar);''')

user_table_create = ('''CREATE TABLE IF NOT EXISTS users(
                user_id varchar,
                first_name varchar,
                last_name varchar,
                gender varchar,
                level varchar);''')

song_table_create = ('''CREATE TABLE IF NOT EXISTS songs(
                song_id varchar CONSTRAINT pk_songs PRIMARY KEY,
                title varchar,
                artist_id varchar NOT NULL,
                year integer,
                duration float);''')

artist_table_create = ('''CREATE TABLE IF NOT EXISTS artists(
                artist_id varchar CONSTRAINT pk_artists PRIMARY KEY,
                name varchar,
                location varchar,
                latitude float,
                longitude float);''')

time_table_create = ('''CREATE TABLE IF NOT EXISTS time(
                start_time timestamp CONSTRAINT pk_time PRIMARY KEY,
                hour integer,
                day integer,
                week integer,
                month integer,
                year integer,
                weekday integer);''')

# STAGING TABLES

staging_events_copy = (''' COPY staging_events
                            FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            REGION 'us-west-2'
                            JSON AS {}
                            TIMEFORMAT 'epochmillisecs'
                            ''').format(config.get('S3', 'LOG_DATA'),
                                        config.get('IAM_ROLE', 'ARN'),
                                        config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (''' COPY staging_songs
                            FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            REGION 'us-west-2'
                            JSON AS 'auto'
                            ''').format(config.get('S3', 'SONG_DATA'),
                                        config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ('''INSERT INTO songplays
                         (start_time,
                          user_id,
                          level,
                          song_id,
                          artist_id,
                          session_id,
                          location,
                          user_agent)
                                         SELECT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
                                         user_id,
                                         level,
                                         s.song_id,
                                         s.artist_id,
                                         session_id,
                                         location,
                                         user_agent
                         FROM staging_events AS e
                         LEFT JOIN staging_songs AS s
                         ON e.artist = s.artist_name
                         AND e.song = s.title
                         AND e.length = s.duration
                         AND page = 'NextSong';''')

user_table_insert = ('''INSERT INTO users
                        (user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                        SELECT DISTINCT user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level
                        FROM staging_events
                        WHERE user_id NOT IN (SELECT DISTINCT user_id FROM users)
                        AND page = 'NextSong';''')

song_table_insert = ('''INSERT INTO songs
                        (song_id,
                        title,
                        artist_id,
                        year,
                        duration)
                        SELECT DISTINCT song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                        FROM staging_songs
                        WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);''')

artist_table_insert = ('''INSERT INTO artists
                        (artist_id,
                        name,
                        location,
                        latitude,
                        longitude)
                        SELECT DISTINCT artist_id,
                                        artist_name,
                                        artist_location,
                                        artist_latitude,
                                        artist_longitude
                        FROM staging_songs
                        WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);''')

time_table_insert = ('''INSERT INTO time
                        (start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                        SELECT a.start_time,
                        EXTRACT (HOUR FROM a.start_time),
                        EXTRACT (DAY FROM a.start_time),
                        EXTRACT (WEEK FROM a.start_time),
                        EXTRACT (MONTH FROM a.start_time),
                        EXTRACT (YEAR FROM a.start_time),
                        EXTRACT (WEEKDAY FROM a.start_time)
                        FROM songplays AS a;''')

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

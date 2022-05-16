import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#DROP SCHEMAS
staging_drop = "DROP SCHEMA IF EXISTS staging CASCADE;"
final_drop = "DROP SCHEMA IF EXISTS final CASCADE"

# DROP STAGING TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs;"

# DROP FINAL TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


#CREATE SCHEMAS
staging_create = "CREATE SCHEMA IF NOT EXISTS staging;"
final_create = "CREATE SCHEMA IF NOT EXISTS final"

# CREATE STAGING
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging.events (artist VARCHAR, auth VARCHAR, firstName VARCHAR, gender VARCHAR, itemInSession INT, lastName VARCHAR, length REAL, level VARCHAR, location VARCHAR, method VARCHAR, page VARCHAR, registration REAL, sessionId INT, song VARCHAR, status INT, ts BIGINT, userAgent VARCHAR, userId INT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging.songs (num_songs INT, artist_id VARCHAR, artist_latitude REAL, artist_longitude REAL, artist_location VARCHAR, artist_name VARCHAR, song_id VARCHAR, title VARCHAR, duration REAL, year INT);
""")

# CREATE FINAL
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS final.songplays (songplay_id INT IDENTITY(1, 1), start_time BIGINT, user_id VARCHAR, \
                        level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id INT, location VARCHAR, user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS final.users \
                        (user_id INT, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR);
""")

time_table_create =  ("""CREATE TABLE IF NOT EXISTS final.time  \
                        (start_time TIMESTAMP WITH TIME ZONE, hour INT, day INT, week INT, month INT, year INT, weekday VARCHAR);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS final.songs \
                        (song_id VARCHAR PRIMARY KEY, title VARCHAR, artist_id VARCHAR, year INT, duration REAL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS final.artists  \
                        (artist_id VARCHAR, name VARCHAR, location VARCHAR, latitude REAL, longitude REAL);
""")

 

# INSERT FROM S3 TO STAGING TABLES
ARN_ROLE = config.get("IAM_ROLE","ARN")

staging_events_copy = ("""copy staging.events from 's3://udacity-dend/log_data/' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(ARN_ROLE)

staging_songs_copy = ("""copy staging.songs from 's3://udacity-dend/song_data/A/A/A/' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto';
""").format(ARN_ROLE)


# INSERT FROM STAGING TO FINAL TABLES

# INSERT FROM EVENTS
songplay_table_insert = (""" INSERT INTO final.songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT ts, userId, level, song, artist, sessionId, location, userAgent from staging.events;""")

user_table_insert = (""" INSERT INTO final.users SELECT userId, firstName, lastName, gender, level from staging.events;""")


time_table_insert = (""" INSERT INTO final.time \
                    SELECT TO_TIMESTAMP(ts, 'YYYYMMDD') AS time, EXTRACT(hour FROM time), EXTRACT(day FROM time), EXTRACT(week FROM time), EXTRACT(month FROM time), EXTRACT(year FROM time), EXTRACT(weekday FROM time)  FROM staging.events;
""")

# INSERT FROM SONGS
song_table_insert = (""" INSERT INTO final.songs SELECT song_id, title, artist_id, year, duration FROM staging.songs;
""")

artist_table_insert = (""" INSERT INTO final.artists \
                    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging.songs;
""")


# QUERY LISTS
drop_table_queries = [staging_drop, final_drop, staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

create_table_queries = [staging_create, final_create, staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

insert_table_queries = [songplay_table_insert, user_table_insert, time_table_insert, song_table_insert, artist_table_insert]

copy_table_queries = [staging_events_copy, staging_songs_copy]
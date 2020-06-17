# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """CREATE TABLE songplays (songplay_id serial PRIMARY KEY,
                                                   start_time TIME(6),
                                                   user_id int REFERENCES users(user_id),
                                                   level varchar,
                                                   song_id varchar REFERENCES songs(song_id),
                                                   artist_id varchar REFERENCES artists(artist_id),
                                                   session_id int,
                                                   location varchar,
                                                   user_agent varchar)"""

user_table_create = """CREATE TABLE users (user_id int PRIMARY KEY,
                                           first_name varchar,
                                           last_name varchar,
                                           gender char(1),
                                           level varchar)"""

song_table_create = """CREATE TABLE songs (song_id varchar PRIMARY KEY,
                                           title varchar, 
                                           artist_id varchar, 
                                           year int, 
                                           duration numeric(15,10))"""

artist_table_create = """CREATE TABLE artists (artist_id varchar PRIMARY KEY, 
                                               name varchar, 
                                               location varchar, 
                                               latitude numeric(10,5), 
                                               longitude numeric(10,5))"""

time_table_create = """CREATE TABLE time (ts_id serial PRIMARY KEY,
                                          start_time TIME(6) not null, 
                                          hour int not null, 
                                          day int not null, 
                                          week int not null, 
                                          month int not null, 
                                          year int not null, 
                                          weekday int not null)"""

# INSERT RECORDS

songplay_table_insert = "INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

user_table_insert = "INSERT INTO users (user_id, first_name, last_name, gender, level) \
                     VALUES (%s, %s, %s, %s, %s) \
                     ON CONFLICT (user_id) DO UPDATE SET level = excluded.level"

song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration) \
                     VALUES (%s, %s, %s, %s, %s) \
                     ON CONFLICT (song_id) DO NOTHING"

artist_table_insert = "INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                       VALUES (%s, %s, %s, %s, %s) \
                       ON CONFLICT (artist_id) DO NOTHING"

time_table_insert = "INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                     VALUES (%s, %s, %s, %s, %s, %s, %s)" 

# FIND SONGS

song_select = ("""SELECT song_id, songs.artist_id 
                  FROM songs INNER JOIN artists
                  ON songs.artist_id = artists.artist_id
                  WHERE title = %s AND name = %s AND duration = %s""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
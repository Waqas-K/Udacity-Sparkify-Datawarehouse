import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events 
                                 ( 
                                   artist            varchar,
                                   auth              varchar,
                                   firstName         varchar,
                                   gender            varchar,
                                   itemInSession     int,
                                   lastName          varchar,
                                   length            numeric,
                                   level             varchar,
                                   location          varchar,
                                   method            varchar,
                                   page              varchar,
                                   registration      numeric,
                                   sessionId         int,
                                   song              varchar,
                                   status            int,
                                   ts                bigint,
                                   userAgent         varchar,
                                   userId            integer);
                             """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (artist_id          varchar,
                                 artist_latitude    numeric,
                                 artist_location    varchar,
                                 artist_longitude   numeric,
                                 artist_name        varchar,
                                 duration           numeric,
                                 num_songs          int,
                                 song_id            varchar,
                                 title              varchar,
                                 year               int);
                            """)

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays
                        ( song_play_id  int identity(0,1)     PRIMARY KEY,
                          start_time    timestamp             NOT NULL SORTKEY, 
                          user_id       int                   NOT NULL,
                          level         varchar,
                          song_id       varchar               NOT NULL DISTKEY,
                          artist_id     varchar,
                          session_id    int,
                          location      varchar,
                          user_agent    varchar);
                        """)

user_table_create = (""" CREATE TABLE IF NOT EXISTS users
                        ( user_id     int       PRIMARY KEY  SORTKEY,
                          first_name  varchar,
                          last_name   varchar,
                          gender      varchar,
                          level       varchar)
                          diststyle all;
                        """)

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs 
                         ( song_id     varchar   PRIMARY KEY  DISTKEY, 
                           title       varchar,
                           artist_id   varchar,
                           year        int       SORTKEY,
                           duration    numeric);
                     """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists
                           ( artist_id  varchar PRIMARY KEY    SORTKEY,
                             name       varchar,
                             location   varchar,
                             latitude   numeric,
                             longitude  numeric)
                             diststyle all;
                        """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time 
                         ( start_time timestamp  PRIMARY KEY   SORTKEY,
                           hour       int,
                           day        int,
                           week       int,
                           month      int,
                           year       int,
                           weekday    int)
                           diststyle all;
                      """)

# STAGING TABLES

staging_events_copy = (""" COPY staging_events 
FROM '{}' credentials 'aws_iam_role={}' format as json '{}'  STATUPDATE ON region 'us-west-2';
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" COPY staging_songs 
FROM '{}' credentials 'aws_iam_role={}' format as json 'auto'  STATUPDATE ON region 'us-west-2';
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE', 'ROLEARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,
                                                    artist_id,session_id,location,user_agent)
        SELECT
            TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
            
        FROM staging_events e, staging_songs s
        WHERE
            e.song=s.title
            AND e.artist=s.artist_name
            AND e.page = 'NextSong'
        
                          
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            userId,
            firstName,
            lastName,
            gender,
            level 
        FROM staging_events
        WHERE page = 'NextSong';
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = (""" INSERT INTO artists(artist_id,name,location,latitude,longitude)
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs;        
""")

time_table_insert = (""" INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT
            start_time,
            EXTRACT(hour from start_time)    AS hour,
            EXTRACT(day from start_time)     AS day,
            EXTRACT(week from start_time)    AS week,            
            EXTRACT(month from start_time)   AS month,        
            EXTRACT(year from start_time)    AS year,
            EXTRACT(weekday from start_time) AS weekday
        FROM 
           (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
            FROM staging_events);
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

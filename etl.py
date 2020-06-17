import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime


def process_song_file(cur, filepath):
    """
    This procedure process the song_data json file which is given as an argument.
    Reads the file from the filepath which is given as argument
    Loads the data from the file to a pandas dataframe
    Inserts the necessary fields from the dataframe into songs and artists tables
    
    Arguments:
    *cur : cursor to access our Sparkify database
    *filepath : path of the song file to be processed
    """
    
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure process the log_data json file which is given as an argument.
    It filters the log_data to remove rows which are not 'NextSong' action.
    It process the timestamp to split it into individual units and inserts in time table.
    Loads the rows with necessary fields into user table.
    It executes a sql query from queries.py to lookup song_id from songs table and 
    artist_id from artists table and loads the info into songplays table
    
    Arguments:
    *cur : cursor to access our Sparkify database
    *filepath : path of the log file to be processed
    """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit = 'ms')
    
    # insert time data records
    time_data = (t.dt.time, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame({column_labels[i]:time_data[i] for i in range(len(time_data))})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        start_time = datetime.datetime.fromtimestamp(row.ts/ 1e3).strftime('%H:%M:%S.%f')

        # insert songplay record
        songplay_data = (start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    It collects all the json files from the root directory given by filepath.
    Displays to the terminal the number of files it is going to process.
    Each file is passed to the function specified in the func argument.
    Displays to the terminal if a file is processed succesfully or not.
    
    Arguments:
    *cur : cursor to access our Sparkify database
    *conn : connection variable to interface with Sparkify database
    *filepath : root directory from which has the json file
    *func : function to be executed
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This function connects to the Sparkify database and a cursor is created.
    It calls process_data method by passing in the path where the json files are located.
    Once, all the song and log files data is processed and loaded by the process_data method, 
    the connection to the Sparkify database is closed.
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
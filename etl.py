import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Reads the input json files, collects them into a dataframe.
        Then extracts the required attributes and values and loads
        them to songs and artists tables.
       
       Parameters:
       cur(type object): contains cursor to Sparkify Db connection
       filepath(type string): contains path to the data file 
       
       Returns : none
    """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values)
    for line in song_data:
        cur.execute(song_table_insert,line)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_longitude', 'artist_latitude']].values)
    for line in artist_data:
        cur.execute(artist_table_insert, line)


def process_log_file(cur, filepath):
    """Reads the input json files, collects them into a dataframe.
        Then extracts the required attributes, queries data from songs
        and artists tables, joins the values and loads them to time,
        users and songplays tables.
       
       Parameters:
       cur(type object): contains cursor to Sparkify Db connection
       filepath(type string): contains path to the data file 
       
       Returns : none
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts,unit='ms')
    
    # insert time data records
    time_data = ( t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ( 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') 
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

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
            #print("restult:"+str(results))
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        timestamp = pd.to_datetime(row.ts, unit = 'ms')
        songplay_data = (timestamp, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ Fetches the files using abolute path for input path, 
     calculates total number of files and iterates over them
     by passing each file to the input function contained in parameter
     func to process data and store it various tables of Sparkify db.
       
       Parameters:
       cur(type object): contains cursor to Sparkify Db connection
       conn(type object): contains connection object to Sparkify Db
       filepath(type string): contains path to the data files
       func(type string): contains function name to be called on input 
       files.
       
       Returns : none
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
    """This is main function that establishes connection to 
      sparkify database and then calls process_data() function
      which eventually call process_song_file()and process_log_file
      to load data in Sparkify db tables, finally connection is closed 
      at the end after data has been commited.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
# An ETL Reads and processes files from song_data and log_data and loads them into dimensional and fact tables
#===========================================================
#Importing Libraries
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


#==========================================================
def process_song_file(cur, filepath):
    """An ETL that extracts songs and artists data from song file and inserst records into songs and artists dimensional tables.
    INPUT:
        cur - A cursor that will be used to execute queries.
        filepath - JASON object
    OUTPUT:
        songs and artists tables with records inserted.
    """
    
    df = pd.read_json(filepath, lines=True)
    for index, row in df.iterrows():
        
        #songs---------------------------------------
        song_data = (row.song_id, row.title, row.artist_id,
                     row.year, row.duration)
        try:
            cur.execute(song_table_insert, song_data)
        except psycopg2.Error as e:
            print("Error: Inserting row for table: songs")
            print (e)
    #artists--------------------------------------------
        artist_data = (row.artist_id, row.artist_name,
                       row.artist_location,
                       row.artist_latitude,
                       row.artist_longitude)
        try:
            cur.execute(artist_table_insert, artist_data)
        except psycopg2.Error as e:
            print("Error: Inserting row for table: artists")
            print (e)

#=============================================================
def process_log_file(cur, filepath):
    """An ETL that 
    - extracts time, users and songplays data from log_data file     - inserts the records into the time and users dimensional tables and songplays fact table respectively.
    INPUT:
        cur - A cursor that will be used to execute queries.
        filepath - JASON object
    OUTPUT:
        time, users and songplays tables with records inserted.
    """
   
    df = pd.read_json(filepath, lines=True)
    df = df[df.page == 'NextSong']
    
    #time----------------------------------------
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df.copy()
    time_data = (t.ts, t.ts.dt.hour, t.ts.dt.day,
                 t.ts.dt.dayofweek, t.ts.dt.month, t.ts.dt.year, 
                 t.ts.dt.weekday)
    column_labels = ['start_time', 'hour', 'day',
                     'week of year','month', 'year', 'weekday']
    
    time_df = pd.DataFrame(columns=column_labels)
    
    for index, column_label in enumerate(column_labels):
        time_df[column_label] = time_data[index]
        
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Inserting row for table: time")
            print (e)

   #users-----------------------------------
    user_df = df[['userId', 'firstName', 'lastName', 'gender',
                  'level']]

    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Inserting row for table: users")
            print (e)
    #songplays-----------------------------------------
    for index, row in df.iterrows():
        try:
            cur.execute(song_select, (row.song, row.artist,
                                      row.length))
            results = cur.fetchone()
        
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

       
            songplay_data = (row.ts, row.userId, row.level,
                             songid, artistid, row.sessionId,
                             row.location, row.userAgent)
            try:
                cur.execute(songplay_table_insert, songplay_data)
            except psycopg2.Error as e:
                print("Error: Inserting row for table: songplays")
                print (e)
        except psycopg2.Error as e:
            print("Error: Querying for Song ID and Artist ID")
            print (e)

#===========================================================
def process_data(cur, conn, filepath, func):
    """Function gets all files matching extension from directory
    - gets total number of files found
    - iterate over files and process
    INPUT:
        cur - A cursor that will be used to execute queries
        conn - connection to database
        filepath - JASON object
        func - table functions
        
    OUTPUT:
        processed entire data
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

#============================================================
def main():
    """ Connects to Postgres database, executes functions above, creates the fact and dimensional tables.
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb                                user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres                  database")
        print(e)
        
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    process_data(cur, conn, filepath='data/song_data',
                 func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', 
                 func=process_log_file)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Load data from a song file to the song and artist data tables."""
    # Open song file
    df = pd.read_json(filepath, lines=True)
    
    # Insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # Insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 
                           'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
    """Load data from a log file to the time, user, and songplay data tables."""
    # Open log file
    df = pd.read_json(filepath, lines=True)
    
    # Filter by NextSong action
    df = df[df['page'] == 'NextSong']
    
    # Convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # Prepare time data for insertion
    time_data = [(int(tt.timestamp() * 1000), tt.hour, tt.day, tt.isocalendar()[1], 
                  tt.month, tt.year, tt.weekday()) for tt in t]
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=time_data, columns=column_labels)
    
    # Insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, tuple(row))  
    
    # Load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # Insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, tuple(row))  
    
    # Insert songplay records
    for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
                songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'],
                     row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    Iterate over all JSON files in the specified directory and its subdirectories
    and populate data tables in sparkifydb.
    """
    # Get all files matching extension from directory and subdirectories
    all_files = []
    for root, dirs, files in os.walk(filepath):
        for file in glob.glob(os.path.join(root, '*.json')):
            all_files.append(os.path.abspath(file))
    
    # Get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')
    
    # Iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')

def main():
    """
    Establishes connection with the sparkify database and gets cursor to it.
    Runs ETL pipelines.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=root123")
    cur = conn.cursor()

    process_data(cur, conn, filepath=r'C:\Users\Mihai\OneDrive\Desktop\project_etl\data\song_data', func=process_song_file)
    process_data(cur, conn, filepath=r'C:\Users\Mihai\OneDrive\Desktop\project_etl\data\log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()

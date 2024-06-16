import json
import boto3
import pandas as pd
from datetime import datetime
import os
from io import StringIO


def albums(data):
    album_list = []
    for row in data['items']:
    
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_external_urls = row['track']['album']['external_urls']['spotify']
        album_element = {
                'album_id': album_id, 'album_name': album_name, 'album_release_date': album_release_date,
                'album_total_tracks': album_total_tracks, 'album_external_urls': album_external_urls
                }
        album_list.append(album_element)
    
    return album_list
    
def artists(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'external_urls': artist['external_urls']['spotify']}
                    artist_list.append(artist_dict)
                    
    return artist_list
    
def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration_ms = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id': song_id, 'song_name': song_name, 'song_duration_ms': song_duration_ms,
                       'song_url': song_url, 'song_popularity': song_popularity, 'song_added': song_added,
                       'album_id': album_id, 'artist_id': artist_id}
        song_list.append(song_element)
        
    return song_list

def top_tracks_artist(data):
    
    top_tracks_artist_list = []
    
    for top_track in data['tracks']:
        
        artist_id = top_track['artists'][0]['id']
        artist_name = top_track['artists'][0]['name']
        track_name = top_track['name']
        popularity = top_track['popularity']
        duration_ms = top_track['duration_ms']
        song_id = top_track['id']
        album_name = top_track['album']['name']
        total_tracks = top_track['album']['total_tracks']
        album_id = top_track['album']['id']
        release_date = top_track['album']['release_date']
        image_album = top_track['album']['images'][0]['url']
        external_url = top_track['external_urls']['spotify']
        top_tracks_artist = {
            'artist_id': artist_id, 'artist_name': artist_name, 'track_name': track_name, 'popularity': popularity,
            'duration_ms': duration_ms, 'song_id': song_id, 'album_name': album_name, 'total_tracks': total_tracks,
            'album_id': album_id, 'release_date': release_date, 'image_album': image_album, 'external_url': external_url
        }
        
        top_tracks_artist_list.append(top_tracks_artist)
        
    return top_tracks_artist_list


def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-jv'
    key_data_playlist_tracks_raw = 'raw_data/to_processed/data_playlist_tracks_raw/'
    key_data_top_tracks_artist = 'raw_data/to_processed/top_tracks_artist_raw/'
    
    spotify_playlist_tracks_data = []
    spotify_playlist_tracks_key = []
    
    spotify_top_tracks_artist_data = []
    spotify_top_tracks_artist_key = []
    
    for file_playlist_tracks in s3.list_objects(Bucket = Bucket, Prefix = key_data_playlist_tracks_raw)['Contents']:
        
        file_playlist_track_key = file_playlist_tracks['Key']

        if (file_playlist_track_key.split('.')[-1] == 'json'):
            
            response = s3.get_object(Bucket = Bucket, Key = file_playlist_track_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_playlist_tracks_data.append(jsonObject)
            spotify_playlist_tracks_key.append(file_playlist_track_key)
    
    for file_top_tracks_artist in s3.list_objects(Bucket = Bucket, Prefix = key_data_top_tracks_artist)['Contents']:
        
        file_top_tracks_artist_key = file_top_tracks_artist['Key']

        if (file_top_tracks_artist_key.split('.')[-1] == 'json'):
            
            response = s3.get_object(Bucket = Bucket, Key = file_top_tracks_artist_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_top_tracks_artist_data.append(jsonObject)
            spotify_top_tracks_artist_key.append(file_top_tracks_artist_key)    
            
    for data in spotify_playlist_tracks_data:
        
        album_list = albums(data)
        song_list = songs(data)
        artist_list = artists(data)

        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.applymap(lambda x: x.strip().replace('"', '') if isinstance(x, str) else x)
        album_df = album_df.drop_duplicates(subset = ['album_id'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.applymap(lambda x: x.strip().replace('"', '') if isinstance(x, str) else x)
        artist_df = artist_df.drop_duplicates(subset = ['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)
        song_df = song_df.applymap(lambda x: x.strip().replace('"', '') if isinstance(x, str) else x)
        song_df = song_df.drop_duplicates()

        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        song_key = 'transformed_data/song_data/songs_transformed_' + str(datetime.now()) + '.csv'
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index = False, sep = ';')
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = song_key, Body = song_content)
        
        artist_key = 'transformed_data/artist_data/artist_transformed_' + str(datetime.now()) + '.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False, sep = ';')
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = artist_key, Body = artist_content)
        
        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False, sep = ';')
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = album_key, Body = album_content)
        
    concatenated_top_tracks_artist_df = pd.DataFrame()
        
    for data in spotify_top_tracks_artist_data:
        
        top_tracks_artist_list = top_tracks_artist(data)
        
        top_tracks_artist_df = pd.DataFrame.from_dict(top_tracks_artist_list)
        
        concatenated_top_tracks_artist_df = pd.concat([concatenated_top_tracks_artist_df, top_tracks_artist_df])
        concatenated_top_tracks_artist_df = concatenated_top_tracks_artist_df.applymap(lambda x: x.strip().replace('"', '') if isinstance(x, str) else x)
        concatenated_top_tracks_artist_df = concatenated_top_tracks_artist_df.drop_duplicates()
        
    top_tracks_artist_key = 'transformed_data/top_tracks_artist_data/top_tracks_artist_transformed_' + str(datetime.now()) + '.csv'
    top_tracks_artist_buffer = StringIO()
    concatenated_top_tracks_artist_df.to_csv(top_tracks_artist_buffer, index = False, sep = ';')
    top_tracks_artist_content = top_tracks_artist_buffer.getvalue()
    s3.put_object(Bucket = Bucket, Key = top_tracks_artist_key, Body = top_tracks_artist_content)
        
    s3_resource = boto3.resource('s3')
    
    for key in spotify_playlist_tracks_key:
        
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/data_playlist_tracks_raw/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
        
    for key in spotify_top_tracks_artist_key:
        
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/top_tracks_artist_raw/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
        
        
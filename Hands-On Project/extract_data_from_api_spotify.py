import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(
    client_id = client_id,
    client_secret = client_secret
        )
        
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF'
    playlist_URI = playlist_link.split('/')[-1]
    data_playlist_tracks = sp.playlist_tracks(playlist_URI)
    
    client = boto3.client('s3')
    
    artists_ids = []
    for row in data_playlist_tracks['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_id = artist['id']
                    artists_ids.append(artist_id)
                    
    for artist in set(artists_ids):
        
        data_top_tracks_artist = sp.artist_top_tracks(artist)
        
        filename = 'raw_data_top_tracks_artist_' + artist + '_' + str(datetime.now()) + '.json'
        
        client.put_object(
            Bucket = 'spotify-etl-project-jv',
            Key = 'raw_data/to_processed/top_tracks_artist_raw/' + filename,
            Body = json.dumps(data_top_tracks_artist)
            
            )
        
    filename = 'spotify_raw_data_playlist_tracks_' + str(datetime.now()) + '.json'
    
    client.put_object(
        Bucket = 'spotify-etl-project-jv',
        Key = 'raw_data/to_processed/data_playlist_tracks_raw/' + filename,
        Body = json.dumps(data_playlist_tracks)
        )
    
    
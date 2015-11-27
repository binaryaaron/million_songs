"""Index of songs and track_ids
"""
import pandas as pd
from sqlalchemy import create_engine

class SongIndex:

    def __init__(self, dataPath):
        """Pass the path for the directory containing track_metadata.db"""
        metadata_engine = create_engine(
            'sqlite:///'+dataPath+'/track_metadata.db')
        sql = """SELECT song_id, track_id FROM songs"""
        songs = pd.read_sql_query(sql, metadata_engine)
        self.songIndex  = {}
        self.trackIndex = {}
        for song in songs.to_dict(orient="records"):
            tids = self.songIndex.get(song['song_id'], set())
            self.songIndex[song['song_id']] = tids.union({song['track_id']})
            self.trackIndex[song['track_id']] = song['song_id']

    def getTrackIDs(self, song_id):
        """get the set of track_ids associates with this song_id"""
        return self.songIndex[song_id]

    def getSongID(self, track_id):
        """get the song_id associated with this track_id"""
        return self.trackIndex[track_id]


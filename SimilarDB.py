"""
A wrapper around the similars database. 
"""
import pandas as pd
from sqlalchemy import create_engine

class SimilarDB:

    def __init__(self, dataPath):
        """Pass the complete path for the directory where the
        lastfm_similars.db file is.

        """
        sims_engine = create_engine(
            'sqlite:///'+dataPath+'/lastfm_similars.db')
        sql = """SELECT tid, target 
                 FROM similars_src"""
        groupedSims = pd.read_sql_query(sql, sims_engine).groupby(['tid'])
        simsDicts = pd.DataFrame(
            groupedSims['target'].apply(list)).reset_index().to_dict(
                orient="records")
        self.similarsDict = {}
        for track in simsDicts:
            track_similars = makeSimilarsSet(track['target'])
            self.similarsDict[track['tid']] = track_similars

    def getSimilarsSet(self, track_ids):
        """get the set of similar tracks associated with the track or tracks
        provided

        """
        if type(track_ids) is not list: track_ids = [track_ids]
        all_similars = set()
        for tid in track_ids:
            track_similars = self.similarsDict.get(tid, set())
            all_similars = all_similars.union(track_similars)
        return all_similars

def makeSimilarsSet(similarsStrings):
    if len(similarsStrings) > 1:
        print("not going to work")
    sims_list = similarsStrings[0].split(',')[0::2]
    l = len(sims_list)
    top_sims = sims_list[0:int(l/2)]
    return set(top_sims)

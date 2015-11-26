"""A class to wrap the lastfm_tags sqlite database.

Reads in all data required to provide a list of tags for each
track. The set of tracks returned is a subset of the tags for that
track which includes only those the most frequent tags across the
data. (top 500 tags).
"""
import pandas as pd
from sqlalchemy import create_engine

class TagDB:

    def __init__(self, dataPath):
        """Pass the complete path for the directory where the lastfm_tags.db
        file is.

        """
        tag_engine = create_engine(
            'sqlite:///'+dataPath+'/lastfm_tags.db')
        sql = """SELECT tids.tid, tags.tag 
                 FROM tid_tag, tids, tags 
                 WHERE tags.ROWID=tid_tag.tag AND tid_tag.tid=tids.ROWID"""
        groupedTags = pd.read_sql_query(sql, tag_engine).groupby(['tid'])
        tagDicts = pd.DataFrame(
            groupedTags['tag'].apply(list)).reset_index().to_dict(
                orient="records")
        self.tagIndex = getTopTags(dataPath)
        self.tagDict = {}
        for td in tagDicts:
            self.tagDict[td['tid']] = [tag for tag in td['tag']
                                       if tag in self.tagIndex]
        
    def getTagSet(self, track_ids):
        """get the set of tags associated with the track or tracks provided"""
        if type(track_ids) is not list: track_ids = [track_ids]
        tags = set()
        for tid in track_ids:
            track_tags = set(self.tagDict.get(tid, []))
            tags = tags.union(track_tags)
        return tags

    def getTagVector(self, track_ids):
        """returns a list of integers representing the indices of the tags
        that are present. Vectors are one-indexed, and all entries are
        in the range [1:500].

        """
        tagSet = list(self.getTagSet(track_ids))
        tagVector = [self.tagIndex[tag] for tag in tagSet]
        tagVector.sort()
        return tagVector

def getTopTags(dataPath):
    lastfm_tags = open(dataPath + '/lastfm_unique_tags.txt', 'r')
    tagIndex = {}
    for tagRank in range(1, 500):
        [tag, f] = lastfm_tags.readline().split('\t')
        tagIndex[tag] = tagRank
    lastfm_tags.close()
    return tagIndex


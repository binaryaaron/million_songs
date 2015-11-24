from pyspark.context import SparkContext
from pyspark.sql import SQLContext

import sqlite3
import pandas as pd
from sqlalchemy import create_engine

import sys

def printUsage():
    print("""
    cbRecommender.py <full/path/to/artist_similarity.db>
                     <path/to/lastfmJSON/in/hdfs (no hdfs://)>
                     <path/to/track_metadata.db>
    """)

if len(sys.argv) < 4:
    printUsage()
    exit()

######### Global variabls ######### (gross)
# The following variables are broadcast to the spark
# cluster and can be used in the functions below
songTable = 'song_data'
sc = SparkContext('local[*]', 'lastfm_recommender')
sqlContext = SQLContext(sc)

### Set up database connections for metadata and similar artists
### This is starting to get really ugly.
### broadcasting this data is probably not a good idea
artist_engine = create_engine('sqlite:///'+sys.argv[1])
sims = pd.read_sql_query(
    'SELECT * FROM similarity', artist_engine)
# broadcsasting these variables is probably a bad idea since 
# they ar quite big
similars = sc.broadcast(sims.similar)
similar_groups = sc.broadcast(sims.groupby('target').groups)

tagFile = open('lastfm_unique_tags.txt', 'r')
# make tag dictionary available across the cluster.
tags = [tagstr[0] for tagstr in map(lambda ts: ts.split('\t'),
                                    [next(tagFile) for x in xrange(500)])]
tagDictionary = sc.broadcast(tags)
tagFile.close()

######## Functions for feature extraction #########

# make a "vector" with indices corresoinding to values in 
# tagDictionary
def getTagVector(track):
    return {tagDictionary.value[tag]:1 for [tag, f] in track.tags
            if tag in tagDictionary.value}

# Actually... it isn't really necessary to represent the tags as a vector...
# we can use sets
def getTagSet(track):
    return {'track_id':track.track_id,
            'track_tags':[tag for [tag, f] in track.tags
                          if tag in tagDictionary.value]}

def getArtistID(track):
    return track.artist_id

# use the similar artists db to make a similar artists vector
# the set this returns is not integers, it is the actual artist_ids.
def getSimilarArtistsSet(track):
    artist_id = getArtistID(track)
    # if no similars are defined then return an empty list
    sims = similar_groups.value.get(artist_id, [])
    sim_ids = map(lambda r: similars.value[r], sims) + [artist_id]
    return {'track_id':track.track_id,
            'similar_artists':list(set(sim_ids))}

def jaccardSimilarity(setA, setB):
    i = len(setA.intersection(setB))
    u = len(setA.union(setB))
    return i/u

# Note, the elements of tags must be distinct from the elements of
# artists this works now because artists is a set of strings and tags
# is a set of integers.
def combineSets((tags, artists)):
    return tags.union(artists)

if __name__ == '__main__':
    fullJSON   = 'hdfs://' + sys.argv[2]

    metadata_engine = create_engine('sqlite:///'+sys.argv[3])
    artistIDs = sqlContext.createDataFrame(
        pd.read_sql_query('SELECT track_id, artist_id FROM songs',
                          metadata_engine))
    
    trackDF   = sqlContext.jsonFile(fullJSON)
    completeDF = trackDF.join(artistIDs, trackDF.track_id == artistIDs.track_id)
    # cache the complete dataframe since we will be accessing it twice
    completeDF.cache()

    tagSets    = sqlContext.createDataFrame(completeDF.map(getTagSet))
    artistSets = sqlContext.createDataFrame(completeDF.map(getSimilarArtistsSet))

    # create and RDD with all track features.
    trackFeatures = tagSets.join(artistSets,
                                 tagSets.track_id == artistSets.track_id)
    # TODO:
    # 1. function that takes a user and constructs a feature set (ie. tag set)
    #    from their top N songs (or all songs with a normalized play count above
    #    some threshold
    #    a. read in the triplets as a pandas DataFrame
    # 2. function that finds the most similar songs to a given song vector
    
    # save the sets so we can use them again...  also just as a test
    # to make sure that everything works correctly
    #tagsFile = 'hdfs:///users/wfvining/challenge2/tagSets.rdd'
    #tagSets.saveAsTextFile(tagsFile)
    #artistsFile = 'hdfs:///users/wfvining/challenge2/artistSets.rdd'
    #artistSets.saveAsTextFile(artistsFile)


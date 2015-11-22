from pyspark.context import SparkContext
from pyspark.sql import SQLContext

# some things that will be useful globally
songTable = 'song_data'
sc = SparkContext('local[*]', 'lastfm_recommender')
sqlContext = SQLContext(sc)


def makeTagDictionary(tagStrings):
    tags = [tagstr[0] for tagstr in map(lambda ts: ts.split('\t'), tagStrings) 
            if int(tagstr[1]) > 1]
    return dict(zip(tags, range(0, len(tags)-1)))

tagFile = open('lastfm_unique_tags.txt', 'r')
# make tag dictionary available across the cluster.
tagDictionary = sc.broadcast(makeTagDictionary(tagFile.readlines()))
tagFile.close()

# make a "vector" with indices corresoinding to values in 
# tagDictionary
def getTagVector(track):
    return {tagDictionary.value[tag]:1 for [tag, f] in track.tags
            if tag in tagDictionary.value}

# Actually... it isn't really necessary to represent the tags as a vector...
# we can use sets
def getTagSet(track):
    return {tagDictionary.value[tag] for [tag, f] in track.tags
            if tag in tagDictionary.value}

# TODO
def makeSimilarsVector(track):
    raise Exception("undefined")

# Probably don't want to include the artist... unless we maybe break it
# up into a bag of words to account for artists such as "Foo with Bar"
# where Foo and Bar collaborated on some album... 
def makeArtistVector(track):
    raise Exception("undefined")

# use the similar artists db to make a similar artists vector
"""
In [1]: import sqlite3

In [2]: import pandas as pd

In [3]: from sqlalchemy import create_engine

In [4]: artists_engine = create_engine('sqlite:////users/wfvining/challenge2/artist_similarity.db')

In [5]: artists_engine
Out[5]: Engine(sqlite:////users/wfvining/challenge2/artist_similarity.db)

In [6]: pd.read_sql_query('SELECT * FROM sqlite_master WHERE type="table"', artists_engine)
Out[6]: 
    type        name    tbl_name  rootpage  \
0  table     artists     artists         2   
1  table  similarity  similarity      2459   

                                                 sql  
0  CREATE TABLE artists (artist_id text PRIMARY KEY)  
1  CREATE TABLE similarity (target text, similar ...  

In [9]: artists = pd.read_sql_query('SELECT * from artists', artists_engine)

In [10]: artists.head()
Out[10]: 
            artist_id
0  AR002UA1187B9A637D
1  AR003FB1187B994355
2  AR006821187FB5192B
3  AR009211187B989185
4  AR009SZ1187B9A73F4

In [11]: sims = pd.read_sql_query('SELECT * from similarity', artists_engine)

In [12]: sims.head()
Out[12]: 
               target             similar
0  AR002UA1187B9A637D  ARQDOR81187FB3B06C
1  AR002UA1187B9A637D  AROHMXJ1187B989023
2  AR002UA1187B9A637D  ARAGWVR1187B9B749B
3  AR002UA1187B9A637D  AREQVWS1241B9CC0A4
4  AR002UA1187B9A637D  ARHBE351187FB3B0CD
"""
def getSimilarArtists(track):
    raise Exception("undefined")

if __name__ == '__main__':    
    #subsetJSON = 'hdfs:///users/wfvining/challenge2/lastfm_subset_all.json'
    #trainJSON  = 'hdfs:///users/wfvining/challenge2/lastfm_train_all.json'
    fullJSON   = 'hdfs:///users/wfvining/challenge2/lastfm_full.json'
    
    #subsetDF = sqlContext.jsonFile(subsetJSON)
    #trainDF  = sqlContext.jsonFile(trainJSON)
    fullDF   = sqlContext.jsonFile(fullJSON).cache()

    # register the data as a temporary SQL table
    # this will be useful for creating user features later.
    sqlContext.registerDataFrameAsTable(fullDF, songTable)

    tagSets = fullDF.rdd.map(getTagSet)

    # TODO:
    # 1. function that takes a user and constructs a feature set (ie. tag set)
    #    from their top N songs (or all songs with a normalized play count above
    #    some threshold
    #    a. read in the triplets as a pandas DataFrame
    # 2. function that finds the most similar songs to a given song vector
    
    # save the sets so we can use them again...
    # outputFile = 'tagSets.rdd'
    # tagSets.saveAsTextFile(outputFile)


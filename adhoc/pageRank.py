'''
Created on Jun 12, 2018

@author: agautam1
'''
import findspark
import re



def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def compute_contribution(urls, rank):
    for url in urls:
        yield (url, rank/len(urls))
        

if __name__ == '__main__':
    findspark.init("/Users/agautam1/Software/spark/spark-1.6.0-bin-hadoop2.6")
    from pyspark  import SparkContext
    sc = SparkContext(appName="PageRank")
    
    #Read the data file and create the links RDD. This will not change so we have persisted it.
    lines = sc.textFile("/Users/agautam1/Downloads/urldata.txt")
    links = lines.map(lambda urls:parseNeighbors(urls)).groupByKey().persist()
    #print links.collect()
    
    #Initialize the Rank RDD to 1.0
    ranks = links.mapValues(lambda x: 1.0)
    #print ranks.collect()
    
    #Iterate for number of passes
    for i in xrange(20):
        contrib = links.join(ranks)
        #print contrib.collect()
        #[(u'url_3', (u'url_2', 0.5)), (u'url_3', (u'url_1', 0.5)), (u'url_1', (u'url_4', 1.0)), (u'url_4', (u'url_3', 0.5)), (u'url_4', (u'url_1', 0.5)), (u'url_2', (u'url_1', 1.0))]
        #v = contrib.flatMapValues(lambda y: compute_contribution(y[0], y[1]))
        v = contrib.values().flatMap(lambda y: compute_contribution(y[0], y[1])).reduceByKey(lambda x,y: x + y)
        
        ranks = v.mapValues(lambda x: 0.15 + 0.85*x)
        
    
    
    print ranks.collect()
        
    sc.stop()
    
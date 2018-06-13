'''
Created on Jun 12, 2018

@author: agautam1
'''
import findspark

if __name__ == '__main__':

    #findspark.init("/opt/spark")
    findspark.init("/Users/agautam1/Software/spark/spark-1.6.0-bin-hadoop2.6")
    import random
    from pyspark import SparkContext
    
    sc = SparkContext(appName="EstimatePi")
    def inside(p):
        x, y = random.random(), random.random()
        return x*x + y*y < 1
    NUM_SAMPLES = 1000000
    count = sc.parallelize(range(0, NUM_SAMPLES)) \
                 .filter(inside).count()
    print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
    sc.stop()
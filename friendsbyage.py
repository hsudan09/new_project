# This calculate the average number of friends for particular age

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Friends-By-Age")

sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age,numFriends)

lines = sc.textFile("C:\\Users\\hites\\PycharmProjects\\pythonProject\\fakefriends.csv")

rdd = lines.map(parseLine)

totalsByAge = rdd.mapValues(lambda x : (x , 1)).reduceByKey(lambda x,y: (x[0] + y [0], x[1] + y[1]))

averagesByAge = totalsByAge.mapValues(lambda x : x[0]/x[1])

results = averagesByAge.collect()

for result in results:
    print(result)

print(totalsByAge.collect())




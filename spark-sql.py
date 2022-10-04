from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("C:\\Users\\hites\\PycharmProjects\\pythonProject\\fakefriends.csv")

people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("Select * from people where age >=13 and age <=19")

#for teen in teenagers.collect():
#    print(teen)

schemaPeople.groupby("age").count().show()

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\hites\\PycharmProjects\\pythonProject\\fakefriends-header.csv")

friendsByAge = lines.select("age","friends")

friendsByAge.groupby("age").avg("friends")

friendsByAge.groupby("age").avg("friends").sort("age")

friendsByAge.groupby("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()


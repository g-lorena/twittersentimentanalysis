#import os
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
#import findspark
#findspark.init('/Users/lorenapersonel/Downloads/spark-3.2.1-bin-hadoop3.2-scala2.13')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def preprocessing(lines):
    words = df1.select(explode(split(df1.text, " ")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words


# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity


def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity


def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2").getOrCreate()
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .load()
    #df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    mySchema = StructType([StructField("text", StringType(), True)])
    values = df.select(from_json(df.value.cast("string"), mySchema).alias("test"))

    df1 = values.select("test.*")
    # Preprocess the data
    words = preprocessing(df1)
    # text classification to define polarity and subjectivity
    words = text_classification(words)

    words = words.repartition(1)
    query = words.writeStream.queryName("all_tweets") \
        .outputMode("append").format("console") \
        .start()

    query.awaitTermination()

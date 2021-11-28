from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from lib.logger import Log4j

if __name__=="__main__":
    spark=SparkSession \
        .builder \
        .appName('Streaming Word Count') \
        .master('local[3]') \
        .config("spark.streaming.stopGracefullyOnShutdown","true") \
        .config("spark.sql.shuffle.partitions",3) \
        .getOrCreate()

    logger = Log4j(spark)

    linesDF = spark.readStream \
            .format("socket") \
            .option("host","localhost") \
            .option("port","9999") \
            .load()

    wordsDf = linesDF.select(expr("explode(split(value,'    ')) as word"))
    wordCount = wordsDf.groupBy("word").count()

    countsDf = wordCount.writeStream \
            .format("console") \
            .option("checkPointLocation","chk-point-dir") \
            .outputMode("complete") \
            .start()

    countsDf.awaitTermination()



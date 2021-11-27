from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__=="__main__":
    spark=SparkSession \
        .builder \
        .appName('Streaming Word Count') \
        .master('local[3]') \
        .getOrCreate()

    logger = Log4j(spark)



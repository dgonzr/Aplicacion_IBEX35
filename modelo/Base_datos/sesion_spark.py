from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AplicacionIbex35").getOrCreate()
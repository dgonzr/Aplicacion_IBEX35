import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (SparkSession.builder
 .appName("IBEX35")
 .master("local[*]")
 .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
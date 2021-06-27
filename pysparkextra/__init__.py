import os
import uuid

from pyspark import SparkContext
from pyspark.sql import SparkSession


def spark():
    ss = SparkSession.builder \
        .config("spark.master", f"local[{os.cpu_count()}]") \
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", True) \
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext
    sc.setCheckpointDir("/tmp/" + str(uuid.uuid4()))
    return ss

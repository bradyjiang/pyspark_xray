from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from demo_app.main import *
import time
starttime = time.time()

sconf = SparkConf() \
    .set("spark.dynamicAllocation.enabled", True)\
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
if const.CONST_BOOL_LOCAL_MODE:
    sc = SparkContext("local")
else:
    sc = SparkContext(conf=sconf)

ss = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
ss.sparkContext.setLogLevel("ERROR")

# starts the engine
calc = Calculator(spark_session=ss, spark_context=sc)
run_succeed = calc.run()

calc.stop()

elapsed = time.time() - starttime

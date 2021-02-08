"""
This driver file is intended to demonstrate how to use this library to step into UDF functions passed into Dataframe.mapInPandas function
"""
import pandas as pd
import pyspark
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
from pyspark_xray import utils_m as utils_debugger
from pyspark_xray import const as const_xray

spark = pyspark.sql.SparkSession.builder.appName("test") \
    .master('local[*]') \
    .getOrCreate()
sc = spark.sparkContext

# Create initial dataframe respond_sdf
rdd_list = [('api_1',"{'api': ['api_1', 'api_1', 'api_1'],'A': [1,2,3], 'B': [4,5,6] }"),
            (' api_2', "{'api': ['api_2', 'api_2', 'api_2'],'A': [7,8,9], 'B': [10,11,12] }")]

schema = StructType([
  StructField('url', StringType(), True),
  StructField('content', StringType(), True)
  ])

jsons = sc.parallelize(rdd_list)
respond_sdf = spark.createDataFrame(jsons, schema)

# Pandas UDF
# references: https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#map and https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.mapInPandas
def pandas_function(url_json):
# Here I want to place breakpoint
    for df in url_json:
        yield pd.DataFrame(eval(df['content'][0]))

# Pnadas UDF transformation applied to respond_sdf
# transformed_df = respond_sdf.mapInPandas(pandas_function, "api string, A int, B int")
transformed_df = utils_debugger.wrapper_sdf_mapinvalues(input_sdf=respond_sdf
                                           , func=pandas_function
                                           , spark_session=spark
                                           , debug_flag=const_xray.CONST_BOOL_LOCAL_MODE
                                            , output_schema="api string, A int, B int")
transformed_df.show()

"""
+-----+---+---+
|  api|  A|  B|
+-----+---+---+
|api_1|  1|  4|
|api_1|  2|  5|
|api_1|  3|  6|
|api_2|  7| 10|
|api_2|  8| 11|
|api_2|  9| 12|
+-----+---+---+
"""
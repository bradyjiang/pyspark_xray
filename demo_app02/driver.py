"""
This driver file is intended to demonstrate how to use this library to step into UDF functions passed into Dataframe.mapInPandas function
"""
import pandas as pd
import pyspark
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
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
def pandas_function(url_json):
# Here I want to place breakpoint
    for df in url_json:
        yield pd.DataFrame(eval(df['content'][0]))

# Pnadas UDF transformation applied to respond_sdf
transformed_df = respond_sdf.mapInPandas(pandas_function, "api string, A int, B int")
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
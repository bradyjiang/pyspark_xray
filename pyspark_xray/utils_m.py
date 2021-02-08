"""
spark debugger functions that run on master node of a cluster
"""
import pandas as pd
def wrapper_sdf_mapinvalues(**kwargs):
    """
    pyspark_xray wrapper function of Spark DataFrame.mapInValues which wraps DataFrame.mapInValues on cluster and locally debuggable version on local
    expect input Spark DataFrame
    :param func: pandas UDF function that is passed into Spark DF mapInValues function
    :return: output Spark DataFrame
    """
    input_sdf = kwargs.get("input_sdf", None)
    output_schema = kwargs.get("output_schema", None)
    func = kwargs.get("func", None)
    spark_session = kwargs.get("spark_session", None)
    debug_flag = kwargs.get("debug_flag", False)
    if debug_flag:
        # https://docs.databricks.com/spark/latest/spark-sql/spark-pandas.html#example
        # Convert the Spark DataFrame back to a pandas DataFrame using Arrow
        input_pdf = input_sdf.select("*").toPandas()
        # convert input pandas DataFrame into an iterator of DataFrames, each row become one DataFrame
        list_pdfs = []
        for index, data in input_pdf.iterrows():
            d = data.to_dict()
            pdf = pd.DataFrame([d])
            list_pdfs.append(pdf)
        output_iter = func(iter(list_pdfs))
        # concatenate pandas DataFrames into one combined_pdf
        combined_pdf = pd.concat(output_iter)
        # convert combined pandas DataFrame into a Spark DataFrame
        output_sdf = spark_session.createDataFrame(data=combined_pdf, schema=output_schema)
    else:
        output_sdf = input_sdf.mapInValues(func=func, schema=output_schema)
    return output_sdf

def wrapper_rdd_map(**kwargs):
    """
    pyspark_xray wrapper function of RDD.map which wraps RDD.map on cluster and loop version on local
    expects input RDD contains dicts
    :param func: lambda function that pass into rdd.map
    :return: output Spark RDD
    """
    input_rdd = kwargs.get("input_rdd", None)
    lambda_func = kwargs.get("func", None)
    spark_session = kwargs.get("spark_session", None)
    spark_context = spark_session.sparkContext
    debug_flag = kwargs.get("debug_flag", False)
    if debug_flag:
        list_new=[]
        for tp_orig in input_rdd.collect():
            tp_new = lambda_func(tp_orig)
            list_new.append(tp_new)
        output_rdd=spark_context.parallelize(list_new)
    else:
        output_rdd = input_rdd.map(lambda_func)
    return output_rdd

def wrapper_rdd_mapvalues(**kwargs):
    """
    pyspark_xray wrapper function of RDD.mapValues which wraps RDD.mapValues on cluster and loop version on local
    expect input RDD contains key-value type tuple, each value is a dict
    :param func: lambda function that pass into rdd.mapValue
    :return: output Spark RDD
    """
    input_rdd = kwargs.get("input_rdd", None)
    lambda_func = kwargs.get("func", None)
    spark_session = kwargs.get("spark_session", None)
    spark_context = spark_session.sparkContext
    debug_flag = kwargs.get("debug_flag", False)
    if debug_flag:
        list_new=[]
        for tp_orig in input_rdd.collect():
            key = tp_orig[0]
            value = tp_orig[1]
            value_new = lambda_func(value)
            tp_new=(key, value_new)
            list_new.append(tp_new)
        output_rdd=spark_context.parallelize(list_new)
    else:
        output_rdd = input_rdd.mapValues(lambda_func)
    return output_rdd
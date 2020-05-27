"""
spark debugger functions that run on master node of a cluster
"""
def wrapper_map(**kwargs):
    """
    pyspark_xray wrapper function of RDD.map which wraps RDD.map on cluster and loop version on local
    expects input RDD contains dicts
    :param func: lambda function that pass into rdd.map
    :return: output Spark RDD
    """
    input_rdd = kwargs.get("input_rdd", None)
    lambda_func = kwargs.get("lambda_func", None)
    spark_context = kwargs.get("spark_context", None)
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

def wrapper_mapvalues(**kwargs):
    """
    pyspark_xray wrapper function of RDD.mapValues which wraps RDD.mapValues on cluster and loop version on local
    expect input RDD contains key-value type tuple, each value is a dict
    :param func: lambda function that pass into rdd.mapValue
    :return: output Spark RDD
    """
    input_rdd = kwargs.get("input_rdd", None)
    lambda_func = kwargs.get("lambda_func", None)
    spark_context = kwargs.get("spark_context", None)
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
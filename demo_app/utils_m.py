"""
library functions that run on master node for ingesting and writing data to multiple format and locations, remote or local
"""

def parse_io_path(**kwargs):
    """
    parse a path as input and return a dict of parsed results
    :param
        orig_path
    :return: dict_result with the keys
        url_flag
        csv_flag
        pqt_flag
        target_file_path
        table_type
        table_name
    """
    orig_path = kwargs.get("orig_path", None)
    dict_result = {
        "target_file_path": orig_path
        , "url_flag": orig_path.find('://') >= 0
        , "csv_flag": True
        , "pqt_flag": False
        , "table_type": None
        , "table_name": None
    }
    if dict_result["url_flag"] is False:
        if ".parquet" in orig_path:
            dict_result["csv_flag"] = False
            dict_result["pqt_flag"] = True
            dict_result["target_file_path"] = orig_path.split('.parquet')[0]
        return dict_result
    csv_flag = True
    pqt_flag = False

    url_tokens = orig_path.split('://')

    table_type, table_name = url_tokens
    target_file_path = orig_path
    if table_type == "sf":
        csv_flag = False

    if table_type == "hdfs":
        target_file_path = orig_path.split(':/')[1]

    if ".parquet" in table_name:
        csv_flag = False
        pqt_flag = True
        target_file_path = target_file_path.split('.parquet')[0]
    dict_result["csv_flag"]=csv_flag
    dict_result["pqt_flag"]=pqt_flag
    dict_result["target_file_path"]=target_file_path
    dict_result["table_name"]=table_name
    dict_result["table_type"]=table_type
    return dict_result

def read_pqt_to_df(**kwargs):
    """
    general purpose parquet to spark dataframe
    :param
    spark_session: SparkSession object
    path: path of parquet file
    filter_string: WHERE statement
    :return: spark dataframe
    """
    func_name="read_pqt_to_df"
    spark_session = kwargs.get("spark_session", None)
    path = kwargs.get("path", None)
    filter_string = kwargs.get("filter_string", None)
    if spark_session is None or path is None:
        print("{} error: either spark_session or path is invalid".format(func_name))
        return
    df = spark_session.read.parquet(path)
    if filter_string is not None:
        df.registerTempTable("table_temp")
        df=spark_session.sql("SELECT * FROM table_temp WHERE {}".format(filter_string))
    return df

def read_csv_to_df(**kwargs):
    """
    general purpose csv to spark dataframe with schema and filter string parameters
    :param dict_params: key values
    schema: StructType type object of schema
    spark: SparkSession object
    path: path of CSV file
    header: True or False
    filter_string: WHERE statement
    delimiter: delimiter char of CSV file
    :return: spark dataframe
    """
    delimiter = kwargs.get("delimiter", ",")
    spark_session = kwargs.get("spark", None)
    path = kwargs.get("path", None)
    header = kwargs.get("header", True)
    filter_string = kwargs.get("filter_string", None)
    customSchema = kwargs.get("schema", None)
    if customSchema is None:
        df = spark_session.read \
            .format('com.databricks.spark.csv') \
            .options(header=True, delimiter=delimiter
                     , inferSchema=True) \
            .load(path)
    else:
        df = spark_session.read \
            .format('com.databricks.spark.csv') \
            .options(header=header, delimiter=delimiter)\
            .load(path, schema=customSchema)
    if filter_string is not None:
        df.registerTempTable("table_csv")
        df=spark_session.sql("SELECT * FROM table_csv WHERE "+filter_string)
    return df

def db_table_to_df(**kwargs):
    """
    Read a table (Hive or Redshift) into a Spark dataframe
    :param spark:
    :param table_name:
    :param filter_string:
    :param table_type:
    :param kwargs:
    :return:
    """
    spark_session = kwargs.get("spark", None)
    filter_string = kwargs.get("filter_string", None)
    table_type = kwargs.get("table_type", "snowflake")
    table_name = kwargs.get("table_name", None)
    read_options = kwargs.get("read_options", None) #dict
    filter_string = filter_string if filter_string else '1=1'
    sql_string = 'select * from {table} where {condition}' \
        .format(table=table_name, condition=filter_string)
    print(sql_string)
    df = None
    if table_type in ['snowflake','sf']:
        read_format = 'net.snowflake.spark.snowflake'
        read_options['query'] = sql_string
        df = spark_session.read.format(read_format)
        for k,v in read_options.items():
            df = df.option(k,v)
        df = df.load()
    return df

def data2df_entry(**kwargs):
    """
    a versatile function to ingest different types of data from different types of locations
    csv, parquet data from local drive, hdfs or s3 or Snowflake table.

    :param input_path: file path or table name
        * Single CSV: path to csv file:
            * local: /users/uid/csv_files/test.csv
            * s3: s3://users/uid/csv_files/test.csv
            * hdfs: hdfs://users/uid/csv_files/test.csv
        * Multiple CSV: path to folder that houses csv files
            * local: /users/uid/csv_files
            * s3: s3://users/uid/csv_files
            * hdfs: hdfs://users/uid/csv_files
        * Single Parquet: path to parquet file:
            * local: /users/uid/pqt_files/test.parquet
            * s3: s3://users/uid/pqt_files/test.parquet
            * hdfs: hdfs://users/uid/pqt_files/test.parquet
        * Multiple Parquet: path to folder that houses parquet files
            * local: /users/uid/pqt_files
            * s3: s3://users/uid/pqt_files
            * hdfs: hdfs://users/uid/pqt_files
        * Snowflake table:
            * remote: sf://SB.UID.TEST_TABLE
    :param spark_session: SparkSession object
    :param filter_string: where statement
    :param input_key: name of input file
    :return: Spark dataframe
    """
    input_path = kwargs.get("input_path", None)
    spark_session = kwargs.get("spark_session", None)
    filter_string = kwargs.get("filter_string", None)
    dict_result = parse_io_path(orig_path=input_path)

    if dict_result["url_flag"]:
        if dict_result["csv_flag"]:
            # parameters used for reading CSV
            header = kwargs.get("header", False)
            delimiter = kwargs.get("delimiter", ",")
            customSchema = kwargs.get("schema", None)
            df = read_csv_to_df(schema=customSchema, spark=spark_session, path=dict_result["target_file_path"], delimiter=delimiter, header=header, filter_string=filter_string)
        elif dict_result["pqt_flag"]:
            df = read_pqt_to_df(dict_params={"spark": spark_session
                , "path": dict_result["target_file_path"]
                , "filter_string": filter_string})
        else:
            df = db_table_to_df(spark=spark_session, table_name=dict_result["table_name"], filter_string=filter_string, table_type=dict_result["table_type"])
    else:
        if dict_result["pqt_flag"]:
            df = read_pqt_to_df(dict_params={"spark": spark_session
                , "path": dict_result["target_file_path"]
                , "filter_string": filter_string})
        else:
            # parameters used for reading CSV
            header = kwargs.get("header", True)
            delimiter = kwargs.get("delimiter", ",")
            customSchema = kwargs.get("schema", None)
            df = read_csv_to_df(schema=customSchema, spark=spark_session, path=dict_result["target_file_path"], delimiter=delimiter, header=header, filter_string=filter_string)
    return df
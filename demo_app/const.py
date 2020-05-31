import os
from pyspark_xray import const
from pyspark.sql.types import *

CONST_PATH_DEMO_APP=os.path.dirname(os.path.abspath( __file__ ))

if const.CONST_BOOL_LOCAL_MODE:
    CONST_DICT_INPUT_PATHS_DEFAULT = {
        "input_loans": os.path.join(CONST_PATH_DEMO_APP, "data", "input", "loans.csv")
    }
    CONST_DICT_OUTPUT_PATHS_DEFAULT = {
        "output_loans": os.path.join(CONST_PATH_DEMO_APP, "data", "output", "calc.csv")
    }
else:
    CONST_DICT_INPUT_PATHS_DEFAULT = {
        "input_loans": "sf://SB.USER.DEMO_APP_INPUT_LOANS"
    }
    CONST_DICT_OUTPUT_PATHS_DEFAULT = {
        "output_loans": "sf://SB.USER.DEMO_APP_OUTPUT_LOANS"
    }
CONST_DICT_INPUT_SCHEMA={
    "input_loans": StructType([
        StructField("loan_id", LongType(), True),
        StructField("loan_amt", DoubleType(), True),
        StructField("apr", DoubleType(), True),
        StructField("term_years", IntegerType(), True)
    ])
}
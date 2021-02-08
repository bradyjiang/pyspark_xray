from demo_app import const
from demo_app import utils_s as utils_slave
from pyspark_xray import utils_m as utils_debugger
from pyspark_xray import const as const_xray
from demo_app import utils_m as utils_master

# this app calculates total_cost and monthly payment amount based on input data
# input fields: loan_id, loan_amt, apr, term_years
# output fields: total_cost_amt, mthly_pmt_amt
# bsaed on https://www.bankrate.com/calculators/managing-debt/annual-percentage-rate-calculator.aspx
class Calculator(object):
    def __init__(self, **kwargs):
        self.dict_input = kwargs.get("dict_input_paths", const.CONST_DICT_INPUT_PATHS_DEFAULT)
        self.dict_output = kwargs.get("dict_output_paths", const.CONST_DICT_OUTPUT_PATHS_DEFAULT)
        self.spark_session = kwargs.get("spark_session", None)
        self.spark_context = kwargs.get("spark_context", None)

    def ingest_data(self):
        errors = []
        loan_df = None
        try:
            input_key = "input_loans"
            customSchema = None
            if input_key in const.CONST_DICT_INPUT_SCHEMA.keys():
                customSchema = const.CONST_DICT_INPUT_SCHEMA[input_key]
            loan_df = utils_master.data2df_entry(input_path = self.dict_input["input_loans"], spark_session=self.spark_session, schema=customSchema)
            self._num_loans = loan_df.count()
            print("ingested "+str(self._num_loans)+" loans")
        except Exception as e:
            errors.append('"{}" loans data ingestion error: {}'
                          .format(self.dict_input["input_loans"], str(e)))
        if errors:
            self.stop()
            print("\nResolve {0:.0f} issue(s) before running engine again: {1}".format(len(errors), str(errors)))
        else:
            self.loan_df = loan_df
        return errors

    def stop(self):
        """Stop the Spark context"""
        if self.spark_context is not None:
            self.spark_context.stop()

    def run(self):
        errors = self.ingest_data()
        if errors:
            return False
        print("input = ")
        self.loan_df.show()
        loan_rdd = self.loan_df.repartition(1).rdd.map(lambda x: (x.loan_id, x))
        # calling native RDD.mapValues transformation function
        # lambda function utils_slave.calc_mthly_payment CANNOT be debugged locally
        rdd_pmt = loan_rdd.mapValues(lambda x: utils_slave.calc_mthly_payment(row=x))
        # normalize rdd from key-value studded format to columnized format
        rdd_pmt2 = rdd_pmt.map(lambda x: x[1])
        df_pmt = rdd_pmt2.toDF()
        print("payment output = ")
        df_pmt.show()
        # calling pyspark_xray RDD.mapValues wrapper function
        # lambda function utils_slave.calc_interest CAN be debugged locally
        rdd_int = utils_debugger.wrapper_rdd_mapvalues(input_rdd=loan_rdd
                , func=lambda x: utils_slave.calc_interest(row=x)
                                               , spark_session=self.spark_session
                                               , debug_flag=const_xray.CONST_BOOL_LOCAL_MODE)
        # normalize rdd from key-value studded format to columnized format
        rdd_int2 = rdd_int.map(lambda x: x[1])
        df_int = rdd_int2.toDF()
        print("interest output = ")
        df_int.show()
        return True
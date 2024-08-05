import great_expectations as gx
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.\
                    master("spark://192.168.1.9:7077").\
                    appName("read data from silver datalake").\
                    getOrCreate()

context = gx.get_context()

dataSource = context.sources.add_or_update_spark(
    name="my_spark_datasource"
)

df = spark.read.\
           format('parquet').\
           load(f"../datalake/silver/{datetime.now().strftime('%Y-%m-%d')}/")

dataAsset = dataSource.add_dataframe_asset(
    name="silver_data",
    dataframe=df
)

batchRequest = dataAsset.build_batch_request()

expectation_suite_name = "testing_data"

context.add_or_update_expectation_suite(expectation_suite_name)

validator = context.get_validator(
    batch_request=batchRequest,
    expectation_suite_name=expectation_suite_name
)

validator.expect_column_values_to_not_be_null(column="date")
validator.expect_column_values_to_not_be_null(column="name")
validator.expect_column_values_to_not_be_null(column="points")
validator.expect_column_values_to_not_be_null(column="division")

validator.expect_column_values_to_be_in_set(
    column="division",
    value_set=["IT", "RnD", "sales"]
)

validator.expect_column_values_to_be_between(
    column="points",
    min_value=10,
    max_value=100
)

validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint = context.add_or_update_checkpoint(
    name="my_quickstart_checkpoint",
    validator=validator,
)

checkpoint_result = checkpoint.run()

print(checkpoint_result)
import great_expectations as gx    

class DataValidation:

    def __init__(self, dataSourceName, dataAssetName, expectationSuitName,df):

        self.context = gx.get_context()
        dataSource = self.context.sources.add_or_update_spark(
            name=dataSourceName
        )
        dataAsset = dataSource.add_dataframe_asset(
            name=dataAssetName,
            dataframe=df
        )

        batchRequest = dataAsset.build_batch_request()
        self.context.add_or_update_expectation_suite(expectationSuitName)

        self.validator = self.context.get_validator(
                            batch_request=batchRequest,
                            expectation_suite_name=expectationSuitName
                        )

    def validateColumnIsNotNull(self, columns):

        for column in columns:

            self.validator.expect_column_values_to_not_be_null(column=column)

    def validateCoulmnValue(self, columns, value_lists):

        for column, value_list in zip(columns, value_lists):

            self.validator.expect_column_values_to_be_in_set(
                column=column,
                value_set=value_list
            )

    def validateColumnValueInRange(self, columns, range_lists):

        for column, interval in zip(columns, range_lists):

            self.validator.expect_column_values_to_be_between(
                column=column,
                min_value=interval[0],
                max_value=interval[1]
            )
    
    def validationResult(self, checkpointName):
        
        self.validator.save_expectation_suite(discard_failed_expectations=False)

        checkpoint = self.context.add_or_update_checkpoint(
            name=checkpointName,
            validator=self.validator,
        )

        return checkpoint.run()
from read import ReadData
from validate import DataValidation

df = ReadData().read()
validation = DataValidation(
        dataSourceName="silver_data_lake", 
        dataAssetName="transformed_data", 
        expectationSuitName="data_validation", 
        df=df
    )

validation.validateColumnIsNotNull(
    columns=["date","name","points","division"]
)

validation.validateCoulmnValue(
    columns=["division"],
    value_lists=[
        ["IT", "RnD", "sales"]
    ]
)

validation.validateColumnValueInRange(
    columns=["points"],
    range_lists=[
        [10,100]
    ]
)

validation_result = validation.validationResult(checkpointName="validate_data_result")

print(validation_result)

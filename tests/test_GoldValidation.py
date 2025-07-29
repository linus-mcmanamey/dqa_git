from pyspark.sql import SparkSession, DataFrame
from pytest_fixture_classes import fixture_class
from src.Utilities import UtilityFunctions as utils
from .base_test import BaseDataValidationTest


class LoadedData:
    def __init__(self, source_df: DataFrame, target_df: DataFrame):
        self.source_sdf = source_df
        self.target_sdf = target_df


@fixture_class(name="data_loader")
class DataLoader:
    def __call__(self, spark_session: SparkSession, table_name: str, data_source_name: str) -> LoadedData:
        source_sdf = utils.source_table_dataframe(spark_session, table_name, data_source_name)
        target_sdf = utils.target_table_dataframe(spark_session, table_name, data_source_name)
        return LoadedData(source_sdf, target_sdf)


class TestBronzeValidation(BaseDataValidationTest):
    pass

import pytest
import os
from typing import List
from pyspark.sql import SparkSession, DataFrame
from chispa import FormattingConfig
from loguru import logger
from _pytest.logging import LogCaptureFixture
from src.Utilities import UtilityFunctions as utls

# from src.MetadataBroadcastManager import MetadataBroadcastManager
from icecream import ic

ic.configureOutput(includeContext=True, contextAbsPath=True)


class LoadedData:
    def __init__(self, source_df: DataFrame, target_df: DataFrame):
        self.source_sdf = source_df
        self.target_sdf = target_df


def pytest_addoption(parser):
    parser.addoption("--table_name", action="store", default="default_table", help="name of the table to test")
    parser.addoption("--test_plan_id", action="store", default="test_plan_id", help="test plan id for Azure DevOps")
    parser.addoption("--pipeline_name", action="store", default="pipeline_name", help="name of the deployment process")
    parser.addoption("--database_name", action="store", default="database_name", help="name of the database to test")
    parser.addoption("--server_address", action="store", default="server_address", help="name of the server_address to test")


@pytest.fixture(autouse=True)
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(caplog.handler, format="{message}", level=0, filter=lambda record: record["level"].no >= caplog.handler.level, enqueue=False)
    yield caplog
    logger.remove(handler_id)


@pytest.fixture
def reportlog(pytestconfig):
    logging_plugin = pytestconfig.pluginmanager.getplugin("logging-plugin")
    handler_id = logger.add(logging_plugin.report_handler, format="{message}")
    yield
    logger.remove(handler_id)


@pytest.fixture(scope="session")
def spark_session():
    spark = None
    spark_config = utls.get_dynamic_spark_config()
    try:
        spark = (
            SparkSession.builder.appName("Unify-DM-Testing")
            .master("local[1]")
            .config("spark.driver.memory", spark_config["driver_memory"])
            .config("spark.executor.memory", spark_config["executor_memory"])
            .config("spark.executor.cores", str(spark_config["executor_cores"]))
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.0,com.azure:azure-storage-blob:12.25.1,com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11")
            .config(f"spark.hadoop.fs.azure.account.key.{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net", os.getenv("AZURE_STORAGE_KEY_SP"))
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL"))
        logger.info("Spark session created successfully")
        yield spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception as stop_error:
                logger.warning(f"Error stopping Spark session: {stop_error}")


# @pytest.fixture(scope="session")
# def metadata_broadcast_manager(spark_session):
#     manager = MetadataBroadcastManager(spark_session)
#     yield manager
#     manager.clear_cache()


@pytest.fixture(scope="module")
def databases_in_scope() -> List[str]:
    databases = []
    if os.getenv("SOURCE_DATABASE"):
        databases.append(os.getenv("SOURCE_DATABASE"))
    if os.getenv("TARGET_DATABASE"):
        databases.append(os.getenv("TARGET_DATABASE"))
    if not databases:
        databases = ["default_test_db"]
    return databases


# @pytest.fixture(scope="function")
# def database_metadata_broadcast(metadata_broadcast_manager, request):
#     database_name = getattr(request, "param", os.getenv("SOURCE_DATABASE", "default_test_db"))
#     return metadata_broadcast_manager.get_database_metadata_broadcast(database_name)


# @pytest.fixture(scope="function")
# def table_metadata_from_broadcast(metadata_broadcast_manager, request):
#     params = getattr(request, "param", {})
#     database_name = params.get("database_name", os.getenv("SOURCE_DATABASE", "default_test_db"))
#     table_name = params.get("table_name", "default_table")
#     schema_name = params.get("schema_name", "dbo")
#     return metadata_broadcast_manager.get_table_metadata_dict_from_broadcast(database_name, table_name, schema_name)


# class BroadcastMetadataHelper:
#     def __init__(self, metadata_broadcast_manager: MetadataBroadcastManager):
#         self.manager = metadata_broadcast_manager

#     def get_table_metadata_fast(self, database_name: str, table_name: str, schema_name: str = "dbo") -> dict:
#         return self.manager.get_table_metadata_dict_from_broadcast(database_name, table_name, schema_name)

#     def get_table_metadata_df_fast(self, database_name: str, table_name: str, schema_name: str = "dbo") -> List[Dict]:
#         return self.manager.get_table_metadata_from_broadcast(database_name, table_name, schema_name)

#     def get_all_tables_in_database(self, database_name: str, schema_name: str = "dbo") -> List[str]:
#         broadcast_var = self.manager.get_database_metadata_broadcast(database_name, schema_name)
#         metadata_rows = broadcast_var.value
#         # Get unique table names from the list of dictionaries
#         table_names = list(set(row["table_name"] for row in metadata_rows))
#         return table_names

#     def get_foreign_key_columns_fast(self, database_name: str, table_name: str, schema_name: str = "dbo") -> List[dict]:
#         metadata_dict = self.get_table_metadata_fast(database_name, table_name, schema_name)
#         return [col for col in metadata_dict["columns"] if col["foreign_key"]]

#     def get_primary_key_columns_fast(self, database_name: str, table_name: str, schema_name: str = "dbo") -> List[str]:
#         metadata_dict = self.get_table_metadata_fast(database_name, table_name, schema_name)
#         return [col["name"] for col in metadata_dict["columns"] if col["is_primary_key"]]

#     def refresh_database_metadata(self, database_name: str, schema_name: str = "dbo"):
#         self.manager.clear_cache(database_name, schema_name)
#         return self.manager.get_database_metadata_broadcast(database_name, schema_name)


# @pytest.fixture(scope="session")
# def broadcast_metadata_helper(metadata_broadcast_manager):
#     return BroadcastMetadataHelper(metadata_broadcast_manager)


@pytest.fixture
def chispa_formats():
    return FormattingConfig(mismatched_rows={"color": "light_yellow"}, matched_rows={"color": "cyan", "style": "bold"}, mismatched_cells={"color": "purple"}, matched_cells={"color": "blue"})


@pytest.fixture
def table_name(request):
    return request.config.getoption("--table_name")


@pytest.fixture
def test_plan_id(request):
    return request.config.getoption("--test_plan_id")


@pytest.fixture
def pipeline_name(request):
    return request.config.getoption("--pipeline_name")


@pytest.fixture
def server_address(request):
    return request.config.getoption("--server_address")


@pytest.fixture
def database_name(request):
    return request.config.getoption("--database_name")


# test_results = []


# def pytest_runtest_makereport(item, call):
#     if call.when == "call":
#         test_case_marker = item.get_closest_marker("test_case_id")
#         if test_case_marker:
#             test_case_id = test_case_marker.args[0]
#             outcome = "Passed" if call.excinfo is None else "Failed"
#             test_results.append({"test_case_id": test_case_id, "outcome": outcome, "test_name": item.name})


def pytest_collection_modifyitems(config, items):
    database_name = config.getoption("--database_name", default="unknown_db")
    table_name = config.getoption("--table_name", default="unknown_table")
    for item in items:
        if "database_name" in item.fixturenames:
            if not hasattr(item, "callspec"):
                continue
            db_name = getattr(item.callspec, "params", {}).get("database_name")
            if db_name:
                item.add_marker(pytest.mark.parametrize_db(database_name=db_name))
        original_name = item.originalname if hasattr(item, "originalname") else item.name
        new_name = f"{original_name} - [{database_name}.{table_name}]"
        item.name = new_name
        item._nodeid = item._nodeid.replace(original_name, new_name)


# def pytest_generate_tests(metafunc):
#     if "database_name" in metafunc.fixturenames:
#         # Priority: 1) Command line args, 2) Environment variables, 3) Defaults
#         cmd_database = metafunc.config.getoption("--database_name", default=None)
#         if cmd_database and cmd_database != "database_name":  # Not the default
#             databases = [cmd_database]
#         else:
#             databases = []
#             if os.getenv("SOURCE_DATABASE"):
#                 databases.append(os.getenv("SOURCE_DATABASE"))
#             if os.getenv("TARGET_DATABASE"):
#                 databases.append(os.getenv("TARGET_DATABASE"))
#             if not databases:
#                 databases = ["default_test_db"]
#         metafunc.parametrize("database_name", databases, scope="module")
#     if "table_name" in metafunc.fixturenames and "database_name" in metafunc.fixturenames:
#         # Priority: 1) Command line args (from main script), 2) Environment variables (fallback)
#         cmd_table = metafunc.config.getoption("--table_name", default=None)
#         if cmd_table and cmd_table != "default_table":  # Command line takes priority
#             test_tables = [cmd_table]
#         else:
#             # Fallback to environment variable for multi-table testing
#             test_tables = os.getenv("TEST_TABLES", "").split(",") if os.getenv("TEST_TABLES") else []
#             test_tables = [table.strip() for table in test_tables if table.strip()]
#         if test_tables:  # Only parametrize if we have tables
#             metafunc.parametrize("table_name", test_tables)
#         else:
#             metafunc.parametrize("table_name", ["__no_table__"])

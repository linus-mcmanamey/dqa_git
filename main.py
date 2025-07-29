import os
import sys
from loguru import logger
import subprocess
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
from icecream import ic

ic.configureOutput(includeContext=True, contextAbsPath=True)
load_dotenv()

logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO", colorize=True, enqueue=True)
os.makedirs(str(Path.cwd().joinpath("pipeline_output", "logs")), exist_ok=True)
logger.add(f"{str(Path.cwd().joinpath('pipeline_output', 'logs', 'main.log'))}", format="{time} {level} {message}", level="DEBUG", rotation="10 MB", enqueue=True, colorize=True)


def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
    ic("uncaught exception, application will terminate.", exc_info=(exc_type, exc_value, exc_traceback))
    logger.critical("uncaught exception, application will terminate.", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_uncaught_exception()


def main():
    ic("************************************************************************************************")
    ic(len(os.getenv("AZURE_STORAGE_KEY_SP")))
    ic("************************************************************************************************")
    sys.path.append(str(Path.cwd()))
    build_name = os.getenv("PIPELINE_NAME", "default_pipeline_name")
    databases_string = str(os.getenv("DATABASES_IN_SCOPE"))
    databases_string = databases_string.replace('"', "")
    logger.info(f"Environment variable PIPELINE_NAME: {build_name}")
    logger.info(f"Environment variable DATABASES_IN_SCOPE: {databases_string}")
    databases_in_scope = [db.strip() for db in databases_string.split(",")]
    for database_name in databases_in_scope:
        data_source_name = database_name.split("_")[0]
        logger.info(f"data_source_name: {data_source_name}")
        table_names_string = os.getenv(f"{data_source_name.upper()}_IN_SCOPE")
        table_names_string = table_names_string.replace('"', "")
        table_name_list = [table.strip() for table in table_names_string.split(",")]
        for table_name in table_name_list:
            logger.info(f"table_name: {table_name}")
            report_directory = Path.cwd().joinpath("test_output", database_name, table_name)
            os.makedirs(report_directory, exist_ok=True)
            xml_file_path = f"{build_name}_{database_name}_{table_name}.xml"
            html_file_path = f"{build_name}_{database_name}_{table_name}.html"
            full_xml_path = report_directory.joinpath(xml_file_path)
            full_html_path = report_directory.joinpath(html_file_path)
            test_env = os.environ.copy()
            test_env["SOURCE_DATABASE"] = database_name
            test_env["PIPELINE_STAGE"] = build_name
            test_env["TABLE_NAME"] = table_name
            # Pass through Azure pipeline environment variables
            azure_env_vars = ["KEY_VAULT_NAME", "SOURCE_SERVER", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID"]
            for var in azure_env_vars:
                if var in os.environ:
                    test_env[var] = os.environ[var]
            test_file_path = Path.cwd().joinpath("tests", f"test_{build_name}.py")
            pytest_command = [
                "python3",
                "-m",
                "pytest",
                "-q",
                "--capture=fd",
                "--color=no",
                "--tb=line",
                "--diff-width=240",
                "--junitxml",
                str(full_xml_path),
                "--html",
                str(full_html_path),
                "--self-contained-html",
                "--disable-warnings",
                str(test_file_path),
                f"--pipeline_name={build_name}",
                f"--server_address={test_env['SOURCE_SERVER']}",
                f"--database_name={database_name}",
                f"--table_name={table_name}",
            ]
            try:
                logger.info(f"Running pytest command: {' '.join(pytest_command)}")
                logger.info(f"Test environment: SOURCE_DATABASE={test_env.get('SOURCE_DATABASE')}")
                logger.info(f"Test environment: KEY_VAULT_NAME={test_env.get('KEY_VAULT_NAME')}")
                logger.info(f"Test environment: SOURCE_SERVER={test_env.get('SOURCE_SERVER')}")
                subprocess.run(pytest_command, check=True)
                logger.success(f"Tests for {database_name}.{table_name} completed successfully. XML: {full_xml_path}, HTML: {full_html_path}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Pytest execution failed: {e}")
                logger.error(f"Return value: {e.returncode}")
                continue


if __name__ == "__main__":
    main()


# import os
# import sys
# from icecream import ic
# from dotenv import load_dotenv
# from pyspark.sql import SparkSession
# from icecream import ic

# ic.configureOutput(includeContext=True, contextAbsPath=True)
# load_dotenv()


# def print_azure_env_vars():
#     """Print only Azure-related environment variable keys"""
#     azure_keys = [key for key in os.environ.items()]
#     for key in sorted(azure_keys):
#         print(key)


# # Usage


# def main():
#     ic("************************************************************************************************")
#     ic(len(os.getenv("AZURE_STORAGE_KEY_SP")))
#     ic("************************************************************************************************")
#     os.environ["STORAGE_KEY"] = sys.argv[1]
#     ic(len(os.getenv("STORAGE_KEY")))
#     ic("************************************************************************************************")
#     spark = (
#         SparkSession.builder.appName("AzureADLS")
#         .master("local[1]")
#         .config("spark.sql.adaptive.enabled", "false")
#         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.0,com.azure:azure-storage-blob:12.25.1")
#         .config(f"spark.hadoop.fs.azure.account.key.{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net", os.getenv("AZURE_STORAGE_KEY_SP"))
#         .getOrCreate()
#     )

#     container = "bronze-layer"
#     path_to_file = "legacy_ingestion/fvms_clean/dbo/weapon"

#     # Read from ADLS
#     sdf = spark.read.parquet(f"abfss://{container}@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/{path_to_file}")
#     ic(sdf.show())

#     path = "abfss://synapse-fs@auedatamigdevlake.dfs.core.windows.net/synapse/workspaces/auedatamigdevsynws/warehouse/bronze_fvms.db/b_fvms_address"
#     sdf = spark.read.parquet(path)
#     ic(sdf.show())


# if __name__ == "__main__":
#     main()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import concat_ws, sha2, md5, col, coalesce, lit, when, collect_list
from typing import List, Dict, Any
from loguru import logger
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import psutil
import platform
import pytest
from pathlib import Path
import yaml
import pandas as pd


class UtilityFunctions:
    @staticmethod
    def get_connection_properties():
        return {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "isolationLevel": "READ_UNCOMMITTED", "encrypt": "true", "trustServerCertificate": "true"}

    @staticmethod
    def get_jdbc_url(database: str) -> str:
        return f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database};{UtilityFunctions.get_connection_properties()}"

    @staticmethod
    def get_settings_from_yaml(file_path: str) -> Dict:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)

    @staticmethod
    def add_row_hash(sdf: DataFrame) -> DataFrame:
        hash_algorithm = "sha256"
        separator = "|"
        null_value = "__NULL__"
        columns_to_hash = sdf.columns
        string_cols = [coalesce(col(c).cast("string"), lit(null_value)).alias(c) for c in columns_to_hash]
        concatenated = concat_ws(separator, *[col(c) for c in string_cols])
        # Apply hash function
        if hash_algorithm == "sha256":
            hash_col = sha2(concatenated, 256)
        elif hash_algorithm == "md5":
            hash_col = md5(concatenated)
        else:
            raise ValueError(f"Unsupported hash algorithm: {hash_algorithm}")
        logger.info(f"Adding row hash using {hash_algorithm} algorithm with separator '{separator}' and null value '{null_value}'")
        return sdf.withColumn("row_hash", hash_col)

    @staticmethod
    def get_environment_info():
        cpu_count = psutil.cpu_count(logical=True) or 4
        memory_gb = psutil.virtual_memory().total / (1024**3)
        disk_usage = psutil.disk_usage("/")
        env_info = {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "cpu_count_logical": cpu_count,
            "cpu_count_physical": psutil.cpu_count(logical=False) or 2,
            "memory_total_gb": round(memory_gb, 2),
            "memory_available_gb": round(psutil.virtual_memory().available / (1024**3), 2),
            "disk_total_gb": round(disk_usage.total / (1024**3), 2),
            "disk_free_gb": round(disk_usage.free / (1024**3), 2),
            "is_azure_devops": os.getenv("AGENT_NAME") is not None or os.getenv("BUILD_BUILDID") is not None,
            "agent_name": os.getenv("AGENT_NAME", "Local"),
            "build_id": os.getenv("BUILD_BUILDID", "N/A"),
        }
        return env_info

    @staticmethod
    def get_dynamic_spark_config():
        cpu_count = psutil.cpu_count(logical=True) or 4  # fallback to 4 if None
        memory_gb = psutil.virtual_memory().total / (1024**3)
        is_azure_devops = os.getenv("AGENT_NAME") is not None or os.getenv("BUILD_BUILDID") is not None
        memory_reserve_factor = 0.6 if is_azure_devops else 0.75
        available_memory_gb = memory_gb * memory_reserve_factor
        cpu_factor = 0.6 if is_azure_devops else 0.75
        executor_cores = max(2, min(int(cpu_count * cpu_factor), cpu_count - 1))
        driver_memory_gb = min(4, available_memory_gb * 0.3)
        executor_memory_gb = max(1, (available_memory_gb - driver_memory_gb) * 0.7)
        num_executors = max(1, executor_cores // 2)
        executor_memory_per_instance = max(1, executor_memory_gb / num_executors)
        config = {
            "executor_cores": executor_cores,
            "executor_memory": f"{int(executor_memory_per_instance)}g",
            "driver_memory": f"{int(driver_memory_gb)}g",
            "executor_instances": num_executors,
            "master": f"local[{executor_cores}]",
            "is_azure_devops": is_azure_devops,
        }
        logger.info(f"Environment: {'Azure DevOps' if is_azure_devops else 'Local'}")
        logger.info(f"System specs: CPU cores={cpu_count}, Memory={memory_gb:.1f}GB")
        logger.info(f"Spark config: cores={executor_cores}, driver_memory={config['driver_memory']}, executor_memory={config['executor_memory']}")
        return config

    @staticmethod
    def sql_server_dataframe(spark_session: SparkSession, table_name: str, database_name: str) -> DataFrame:
        try:
            client = SecretClient(vault_url=f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/", credential=DefaultAzureCredential())
            jdbc_url = f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database_name};encrypt=true;trustServerCertificate=true;"
            password_secret = client.get_secret("atldm-sql-atlassbxsql02")
            return (
                spark_session.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", "atldm-sql-atlassbxsql02")
                .option("password", password_secret.value)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
            )
        except Exception as e:
            pytest.fail(f"SQL Server connection failed: {str(e)}")

    @staticmethod
    def temp_storage_dataframe(spark_session: SparkSession, file_name: str, data_source_name: str) -> DataFrame:
        try:
            temp_path = str(os.getenv("TEMP_TEST_FILES"))
            container_path = str(os.getenv("CONTAINER_PATH"))
            if "cms" in data_source_name.lower():
                data_source_name = "cms_clean"
            elif "fvms" in data_source_name.lower():
                data_source_name = "fvms_clean"
            local_path = str(Path(temp_path).joinpath(container_path, data_source_name, "dbo", f"{file_name}"))
            return spark_session.read.parquet(local_path)
        except Exception as e:
            pytest.fail(f"Storage container connection failed: {str(e)}")

    @staticmethod
    def target_table_dataframe(spark_session: SparkSession, file_name: str, data_source_name: str) -> DataFrame:
        try:
            db_prefix = str(os.getenv("CONTAINER_NAME")).split("-")[0]
            first_character = db_prefix[0]
            if "cms" in data_source_name.lower():
                data_source_name = "cms"
            elif "fvms" in data_source_name.lower():
                data_source_name = "fvms"
            database_name = str(db_prefix.lower() + "_" + data_source_name.lower())
            if first_character == "g":
                database_name = "gold_data_model"
                local_path = f"abfss://synapse-fs@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/{str(os.getenv('SYNAPSE_CONTAINER_PATH'))}/{database_name}.db/{file_name}"
            else:
                local_path = f"abfss://synapse-fs@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/{str(os.getenv('SYNAPSE_CONTAINER_PATH'))}/{database_name}.db/{first_character}_{data_source_name}_{file_name}"
            return spark_session.read.parquet(local_path)
        except Exception as e:
            pytest.fail(f"Storage container connection failed: {str(e)}")

    def source_table_dataframe(spark_session: SparkSession, file_name: str, data_source_name: str) -> DataFrame:
        try:
            db_prefix = str(os.getenv("CONTAINER_NAME")).split("-")[0]
            first_character = db_prefix[0]
            if "cms" in data_source_name.lower():
                data_source_name = "cms"
            elif "fvms" in data_source_name.lower():
                data_source_name = "fvms"
            if first_character == "s":
                first_character = "b"
            elif first_character == "g":
                first_character = "s"
            if db_prefix == "silver":
                db_prefix = "bronze"
            if db_prefix == "gold":
                db_prefix = "silver"
            database_name = str(db_prefix.lower() + "_" + data_source_name.lower())
            local_path = f"abfss://synapse-fs@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/{str(os.getenv('SYNAPSE_CONTAINER_PATH'))}/{database_name}.db/{first_character}_{data_source_name}_{file_name}"
            return spark_session.read.parquet(local_path)
        except Exception as e:
            pytest.fail(f"Storage container connection failed: {str(e)}")

    def truncated_table_dataframe(spark_session: SparkSession, file_name: str, data_source_name: str) -> DataFrame:
        try:
            data_source_name = "gold_data_model_truncated"
            local_path = f"abfss://synapse-fs@{os.getenv('STORAGE_ACCOUNT')}.dfs.core.windows.net/{str(os.getenv('SYNAPSE_CONTAINER_PATH'))}/{data_source_name}.db/{file_name}"
            return spark_session.read.parquet(local_path)
        except Exception as e:
            pytest.fail(f"Storage container connection failed: {str(e)}")

    @staticmethod
    def compare_table_schemas(spark_session: SparkSession, table_name: str, source_database: str, target_database: str, schema_name: str = "dbo") -> Dict[str, Any]:
        source_metadata = UtilityFunctions.get_extended_table_metadata_pyspark(spark_session, table_name, source_database, schema_name)
        target_metadata = UtilityFunctions.get_extended_table_metadata_pyspark(spark_session, table_name, target_database, schema_name)
        source_columns = {col["name"]: col for col in source_metadata["columns"]}
        target_columns = {col["name"]: col for col in target_metadata["columns"]}
        common_columns = set(source_columns.keys()).intersection(set(target_columns.keys()))
        source_only_columns = set(source_columns.keys()) - set(target_columns.keys())
        target_only_columns = set(target_columns.keys()) - set(source_columns.keys())
        schema_differences = []
        for col_name in common_columns:
            source_col = source_columns[col_name]
            target_col = target_columns[col_name]
            differences = {}
            if source_col["data_type"] != target_col["data_type"]:
                differences["data_type"] = {"source": source_col["data_type"], "target": target_col["data_type"]}
            if source_col["max_length"] != target_col["max_length"]:
                differences["max_length"] = {"source": source_col["max_length"], "target": target_col["max_length"]}
            if source_col["precision"] != target_col["precision"]:
                differences["precision"] = {"source": source_col["precision"], "target": target_col["precision"]}
            if source_col["scale"] != target_col["scale"]:
                differences["scale"] = {"source": source_col["scale"], "target": target_col["scale"]}
            if source_col["is_nullable"] != target_col["is_nullable"]:
                differences["is_nullable"] = {"source": source_col["is_nullable"], "target": target_col["is_nullable"]}
            if differences:
                schema_differences.append({"column": col_name, "differences": differences})
        schema_comparison = {
            "source_database": source_database,
            "target_database": target_database,
            "table_name": table_name,
            "schema_name": schema_name,
            "common_columns_count": len(common_columns),
            "source_only_columns": list(source_only_columns),
            "target_only_columns": list(target_only_columns),
            "schema_differences": schema_differences,
            "is_schema_match": len(source_only_columns) == 0 and len(target_only_columns) == 0 and len(schema_differences) == 0,
        }
        logger.info(f"Schema comparison complete for {schema_name}.{table_name}: {len(common_columns)} common columns, {len(source_only_columns)} source-only, {len(target_only_columns)} target-only, {len(schema_differences)} differences")
        return schema_comparison

    @staticmethod
    def get_referencing_tables_pyspark(spark_session: SparkSession, table_name: str, database_name: str, schema_name: str = "dbo") -> List[Dict[str, Any]]:
        try:
            client = SecretClient(vault_url=f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/", credential=DefaultAzureCredential())
            password_secret = client.get_secret("atldm-sql-atlassbxsql02")
            jdbc_url = f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database_name};encrypt=true;trustServerCertificate=true;"
            connection_properties = UtilityFunctions.get_connection_properties()
            connection_properties["user"] = "atldm-sql-atlassbxsql02"
            connection_properties["password"] = password_secret.value
            # Get all tables and schemas
            tables_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.tables").options(**connection_properties).load()
            tables_df = tables_df.select("object_id", "name", "schema_id")
            tables_df = tables_df.withColumnRenamed("name", "table_name").withColumnRenamed("object_id", "table_object_id").withColumnRenamed("schema_id", "table_schema_id")
            schemas_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.schemas").options(**connection_properties).load()
            schemas_df = schemas_df.select("name", "schema_id")
            schemas_df = schemas_df.withColumnRenamed("name", "schema_name").withColumnRenamed("schema_id", "schema_schema_id")
            # Get columns info
            columns_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.columns").options(**connection_properties).load()
            columns_df = columns_df.select("object_id", "column_id", "name")
            columns_df = columns_df.withColumnRenamed("name", "column_name").withColumnRenamed("object_id", "column_object_id").withColumnRenamed("column_id", "col_id")
            # Get foreign key relationships
            foreign_key_columns_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.foreign_key_columns").options(**connection_properties).load()
            foreign_key_columns_df = foreign_key_columns_df.select("constraint_object_id", "parent_object_id", "parent_column_id", "referenced_object_id", "referenced_column_id")
            foreign_keys_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.foreign_keys").options(**connection_properties).load()
            foreign_keys_df = foreign_keys_df.select("object_id", "name")
            foreign_keys_df = foreign_keys_df.withColumnRenamed("name", "foreign_key_name").withColumnRenamed("object_id", "fk_object_id")
            # Get the target table info first to filter for it
            target_table_df = tables_df.join(schemas_df, tables_df.table_schema_id == schemas_df.schema_schema_id, "inner").filter((col("table_name") == table_name) & (col("schema_name") == schema_name)).select("table_object_id")
            if target_table_df.count() == 0:
                raise ValueError(f"Table '{schema_name}.{table_name}' not found in database '{database_name}'.")
            target_object_id = target_table_df.collect()[0].table_object_id
            # Find all foreign keys that reference our target table
            referencing_fks_df = foreign_key_columns_df.filter(col("referenced_object_id") == target_object_id).join(foreign_keys_df, foreign_key_columns_df.constraint_object_id == foreign_keys_df.fk_object_id, "inner")
            # Get parent table and column information for referencing tables
            parent_tables_df = (
                tables_df.join(schemas_df, tables_df.table_schema_id == schemas_df.schema_schema_id, "inner")
                .select("table_object_id", "table_name", "schema_name")
                .withColumnRenamed("table_object_id", "parent_table_object_id")
                .withColumnRenamed("table_name", "parent_table_name")
                .withColumnRenamed("schema_name", "parent_schema_name")
            )
            parent_columns_df = (
                columns_df.select("column_object_id", "col_id", "column_name")
                .withColumnRenamed("column_object_id", "parent_column_object_id")
                .withColumnRenamed("col_id", "parent_col_id")
                .withColumnRenamed("column_name", "parent_column_name")
            )
            referenced_columns_df = (
                columns_df.select("column_object_id", "col_id", "column_name")
                .withColumnRenamed("column_object_id", "referenced_column_object_id")
                .withColumnRenamed("col_id", "referenced_col_id")
                .withColumnRenamed("column_name", "referenced_column_name")
            )
            # Join to get complete referencing information
            result_df = (
                referencing_fks_df.join(parent_tables_df, referencing_fks_df.parent_object_id == parent_tables_df.parent_table_object_id, "inner")
                .join(parent_columns_df, (referencing_fks_df.parent_object_id == parent_columns_df.parent_column_object_id) & (referencing_fks_df.parent_column_id == parent_columns_df.parent_col_id), "inner")
                .join(referenced_columns_df, (referencing_fks_df.referenced_object_id == referenced_columns_df.referenced_column_object_id) & (referencing_fks_df.referenced_column_id == referenced_columns_df.referenced_col_id), "inner")
                .select("parent_table_name", "foreign_key_name", "parent_column_name", "referenced_column_name")
            )
            # Group by table and foreign key to aggregate columns

            grouped_df = result_df.groupBy("parent_table_name", "foreign_key_name").agg(collect_list("parent_column_name").alias("constrained_columns"), collect_list("referenced_column_name").alias("referred_columns"))
            referencing_tables = []
            for row in grouped_df.collect():
                referencing_tables.append({"child_table": row.parent_table_name, "foreign_key_name": row.foreign_key_name, "constrained_columns": row.constrained_columns, "referred_columns": row.referred_columns})
            logger.info(f"Found {len(referencing_tables)} tables referencing {schema_name}.{table_name}")
            return referencing_tables
        except Exception as e:
            logger.error(f"Failed to get referencing tables for {schema_name}.{table_name}: {str(e)}")
            raise

    @staticmethod
    def find_orphaned_records_pyspark(spark_session: SparkSession, table_name: str, database_name: str, schema_name: str = "dbo") -> List[Dict[str, Any]]:
        try:
            # Use the new helper function to get referencing tables
            referencing_tables = UtilityFunctions.get_referencing_tables_pyspark(spark_session, table_name, database_name, schema_name)
            # Set up connection for orphaned record queries
            client = SecretClient(vault_url=f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/", credential=DefaultAzureCredential())
            jdbc_url = f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database_name};encrypt=true;trustServerCertificate=true;"
            password_secret = client.get_secret("atldm-sql-atlassbxsql02")
            orphaned_records = []
            for ref_table in referencing_tables:
                child_table = ref_table["child_table"]
                constrained_columns = ref_table["constrained_columns"]
                referred_columns = ref_table["referred_columns"]
                join_conditions = []
                where_conditions = []
                for const_col, ref_col in zip(constrained_columns, referred_columns):
                    join_conditions.append(f"child.{const_col} = parent.{ref_col}")
                    where_conditions.append(f"parent.{ref_col} IS NULL")
                select_columns = ", ".join(constrained_columns)
                join_clause = " AND ".join(join_conditions)
                where_clause = " AND ".join(where_conditions)
                orphaned_query = f"""
                SELECT {select_columns}, COUNT(*) as orphaned_count
                FROM {child_table} child
                LEFT JOIN {schema_name}.{table_name} parent
                    ON {join_clause}
                WHERE {where_clause}
                GROUP BY {select_columns}
                """
                try:
                    orphaned_df = (
                        spark_session.read.format("jdbc")
                        .option("url", jdbc_url)
                        .option("query", orphaned_query)
                        .option("user", "atldm-sql-atlassbxsql02")
                        .option("password", password_secret.value)
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                    )
                    for orphan_row in orphaned_df.collect():
                        orphaned_values = {}
                        for i, col in enumerate(constrained_columns):
                            orphaned_values[col] = orphan_row[i]
                        orphaned_records.append({"child_table": child_table, "foreign_key_columns": constrained_columns, "orphaned_values": orphaned_values, "orphaned_count": orphan_row.orphaned_count})
                except Exception as query_error:
                    logger.info(f"Failed to check orphaned records for table {child_table}: {str(query_error)}")
                    continue
            return orphaned_records
        except Exception as e:
            logger.error(f"Failed to find orphaned records for table {schema_name}.{table_name}: {str(e)}")
            pytest.fail(f"Orphaned records detection failed: {str(e)}")

    @staticmethod
    def get_total_orphaned_count(spark_session: SparkSession, table_name: str, database_name: str, schema_name: str = "dbo") -> int:
        orphaned_records = UtilityFunctions.find_orphaned_records_pyspark(spark_session, table_name, database_name, schema_name)
        total_orphaned = sum(record["orphaned_count"] for record in orphaned_records)
        return total_orphaned

    @staticmethod
    def merge_and_keep_unique_only(df1, df2, first_name, second_name, ignore_cols=["database_name", "foreign_key_name", "referenced_object_id"]):
        df1 = df1.copy()
        df1["database_name"] = first_name
        df2 = df2.copy()
        df2["database_name"] = second_name
        merged = pd.concat([df1, df2], ignore_index=True)
        group_cols = [col for col in merged.columns if col not in ignore_cols]
        counts = merged.groupby(group_cols).transform("size")
        return merged[counts == 1].reset_index(drop=True)

    @staticmethod
    def write_dfs_to_excel(df1, df2, filename, sheet1_name="Sheet1", sheet2_name="Sheet2"):
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            df1.to_excel(writer, sheet_name=sheet1_name, index=False)
            df2.to_excel(writer, sheet_name=sheet2_name, index=False)

    @staticmethod
    def get_extended_table_metadata_pyspark(spark_session: SparkSession, table_name: str, database_name: str, schema_name: str = "dbo") -> Dict[str, Any]:
        try:
            client = SecretClient(vault_url=f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/", credential=DefaultAzureCredential())
            password_secret = client.get_secret("atldm-sql-atlassbxsql02")
            jdbc_url = f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database_name};encrypt=true;trustServerCertificate=true;"
            connection_properties = UtilityFunctions.get_connection_properties()
            connection_properties["user"] = "atldm-sql-atlassbxsql02"
            connection_properties["password"] = password_secret.value
            tables_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.tables").options(**connection_properties).load()
            tables_df = tables_df.select("object_id", "name", "schema_id")
            tables_df = tables_df.withColumnRenamed("name", "table_name").withColumnRenamed("object_id", "table_object_id").withColumnRenamed("schema_id", "table_schema_id")
            schemas_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.schemas").options(**connection_properties).load()
            schemas_df = schemas_df.select("name", "schema_id")
            schemas_df = schemas_df.withColumnRenamed("name", "schema_name").withColumnRenamed("schema_id", "schema_schema_id")
            columns_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.columns").options(**connection_properties).load()
            columns_df = columns_df.select("object_id", "column_id", "name", "user_type_id", "max_length", "precision", "scale", "is_nullable", "is_identity", "default_object_id")
            columns_df = columns_df.withColumnRenamed("name", "column_name").withColumnRenamed("object_id", "column_object_id").withColumnRenamed("column_id", "col_id")
            types_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.types").options(**connection_properties).load()
            types_df = types_df.select("user_type_id", "name")
            types_df = types_df.withColumnRenamed("name", "data_type").withColumnRenamed("user_type_id", "type_user_type_id")
            computed_columns_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.computed_columns").options(**connection_properties).load()
            computed_columns_df = computed_columns_df.select("object_id", "column_id", "definition")
            computed_columns_df = computed_columns_df.withColumnRenamed("object_id", "cc_object_id").withColumnRenamed("column_id", "cc_column_id")
            index_columns_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.index_columns").options(**connection_properties).load()
            index_columns_df = index_columns_df.select("object_id", "index_id", "column_id")
            index_columns_df = index_columns_df.withColumnRenamed("object_id", "ic_object_id").withColumnRenamed("column_id", "ic_column_id")
            indexes_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.indexes").options(**connection_properties).load()
            indexes_df = indexes_df.select("object_id", "index_id", "is_primary_key", "is_unique_constraint")
            indexes_df = indexes_df.withColumnRenamed("object_id", "idx_object_id").withColumnRenamed("index_id", "idx_index_id")
            foreign_key_columns_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.foreign_key_columns").options(**connection_properties).load()
            foreign_key_columns_df = foreign_key_columns_df.select("constraint_object_id", "parent_object_id", "parent_column_id", "referenced_object_id", "referenced_column_id")
            foreign_key_columns_df = (
                foreign_key_columns_df.withColumnRenamed("constraint_object_id", "fkc_constraint_object_id").withColumnRenamed("parent_object_id", "fkc_parent_object_id").withColumnRenamed("parent_column_id", "fkc_parent_column_id")
            )
            foreign_keys_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.foreign_keys").options(**connection_properties).load()
            foreign_keys_df = foreign_keys_df.select("object_id", "name")
            foreign_keys_df = foreign_keys_df.withColumnRenamed("name", "foreign_key_name").withColumnRenamed("object_id", "fk_object_id")
            # Join all DataFrames to create the main DataFrame with extended metadata
            index_constraints_df = index_columns_df.join(indexes_df, (index_columns_df.ic_object_id == indexes_df.idx_object_id) & (index_columns_df.index_id == indexes_df.idx_index_id), "inner").select(
                "ic_object_id", "ic_column_id", "is_primary_key", "is_unique_constraint"
            )
            main_df = (
                tables_df.join(schemas_df, tables_df.table_schema_id == schemas_df.schema_schema_id, "inner")
                .join(columns_df, tables_df.table_object_id == columns_df.column_object_id, "inner")
                .join(types_df, columns_df.user_type_id == types_df.type_user_type_id, "inner")
            )
            result_df = (
                main_df.join(computed_columns_df, (main_df.column_object_id == computed_columns_df.cc_object_id) & (main_df.col_id == computed_columns_df.cc_column_id), "left")
                .join(index_constraints_df, (main_df.column_object_id == index_constraints_df.ic_object_id) & (main_df.col_id == index_constraints_df.ic_column_id), "left")
                .join(foreign_key_columns_df, (main_df.column_object_id == foreign_key_columns_df.fkc_parent_object_id) & (main_df.col_id == foreign_key_columns_df.fkc_parent_column_id), "left")
                .join(foreign_keys_df, foreign_key_columns_df.fkc_constraint_object_id == foreign_keys_df.fk_object_id, "left")
                .filter(main_df.schema_name == schema_name)
                .filter(main_df.table_name == table_name)
                .select(
                    "table_name",
                    "schema_name",
                    "column_name",
                    "data_type",
                    "max_length",
                    "precision",
                    "scale",
                    "is_nullable",
                    "is_identity",
                    "default_object_id",
                    "definition",
                    "is_primary_key",
                    "is_unique_constraint",
                    "foreign_key_name",
                    "referenced_object_id",
                    "referenced_column_id",
                )
                .withColumn("database_name", lit(database_name))
                .withColumn("is_primary_key", when(col("is_primary_key").isNull(), lit(False)).otherwise(col("is_primary_key")))
                .withColumn("is_unique_constraint", when(col("is_unique_constraint").isNull(), lit(False)).otherwise(col("is_unique_constraint")))
            )
            if result_df.count() == 0:
                raise ValueError(f"Table '{schema_name}.{table_name}' not found in database '{database_name}'.")
            columns_list = []
            for row in result_df.collect():
                column_metadata = {
                    "name": row.column_name,
                    "data_type": row.data_type,
                    "max_length": row.max_length,
                    "precision": row.precision,
                    "scale": row.scale,
                    "is_nullable": row.is_nullable,
                    "is_identity": row.is_identity,
                    "default_value": row.default_object_id,
                    "computed_column_definition": row.definition,
                    "is_primary_key": row.is_primary_key if row.is_primary_key else False,
                    "is_unique_constraint": row.is_unique_constraint if row.is_unique_constraint else False,
                    "foreign_key": {
                        "name": row.foreign_key_name,
                        "referenced_table": None,  # Would need additional join to get table name from object_id
                        "referenced_column": None,  # Would need additional join to get column name from column_id
                    }
                    if row.foreign_key_name
                    else None,
                }
                columns_list.append(column_metadata)
            table_metadata = {
                "schema": schema_name,
                "table_name": table_name,
                "database_name": database_name,
                "columns": columns_list,
            }
            logger.info(f"Retrieved extended metadata for table {schema_name}.{table_name} from database {database_name}")
            return table_metadata
        except Exception as e:
            logger.error(f"Failed to get extended metadata for table {schema_name}.{table_name}: {str(e)}")
            raise

    @staticmethod
    def get_table_metadata_as_dataframe(spark_session: SparkSession, table_name: str, database_name: str, schema_name: str = "dbo") -> DataFrame:
        metadata_dict = UtilityFunctions.get_extended_table_metadata_pyspark(spark_session, table_name, database_name, schema_name)
        columns_data = []
        for column in metadata_dict["columns"]:
            columns_data.append(
                {
                    "table_name": metadata_dict["table_name"],
                    "schema_name": metadata_dict["schema"],
                    "column_name": column["name"],
                    "data_type": column["data_type"],
                    "max_length": column["max_length"],
                    "precision": column["precision"],
                    "scale": column["scale"],
                    "is_nullable": column["is_nullable"],
                    "is_identity": column["is_identity"],
                    "default_object_id": column["default_value"],
                    "definition": column["computed_column_definition"],
                    "is_primary_key": column["is_primary_key"],
                    "is_unique_constraint": column["is_unique_constraint"],
                    "foreign_key_name": column["foreign_key"]["name"] if column["foreign_key"] else None,
                    "referenced_schema_name": column["foreign_key"]["referenced_table"] if column["foreign_key"] else None,
                    "referenced_table_name": column["foreign_key"]["referenced_table"] if column["foreign_key"] else None,
                    "referenced_column_name": column["foreign_key"]["referenced_column"] if column["foreign_key"] else None,
                    "database_name": metadata_dict["database_name"],
                }
            )
        return spark_session.createDataFrame(columns_data)

    @staticmethod
    def detect_circular_references(spark_session: SparkSession, database_name: str, schema_name: str = "dbo"):
        client = SecretClient(vault_url=f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/", credential=DefaultAzureCredential())
        password_secret = client.get_secret("atldm-sql-atlassbxsql02")
        jdbc_url = f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database_name};encrypt=true;trustServerCertificate=true;"
        connection_properties = UtilityFunctions.get_connection_properties()
        connection_properties["user"] = "atldm-sql-atlassbxsql02"
        connection_properties["password"] = password_secret.value
        tables_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.tables").options(**connection_properties).load()
        tables_df = tables_df.select("object_id", "name").withColumnRenamed("name", "table_name").withColumnRenamed("object_id", "table_object_id")
        foreign_keys_df = spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.foreign_keys").options(**connection_properties).load()
        foreign_keys_df = foreign_keys_df.select("parent_object_id", "referenced_object_id")
        parent_tables = tables_df.select("table_object_id", "table_name").withColumnRenamed("table_object_id", "parent_object_id").withColumnRenamed("table_name", "parent_table")
        referenced_tables = tables_df.select("table_object_id", "table_name").withColumnRenamed("table_object_id", "referenced_object_id").withColumnRenamed("table_name", "referenced_table")
        fk_relationships = foreign_keys_df.join(parent_tables, "parent_object_id", "inner").join(referenced_tables, "referenced_object_id", "inner").select("parent_table", "referenced_table")
        relationships = [(row.parent_table, row.referenced_table) for row in fk_relationships.collect()]

        def find_cycles(graph, start, visited, rec_stack, path):
            visited.add(start)
            rec_stack.add(start)
            path.append(start)
            if start in graph:
                for neighbor in graph[start]:
                    if neighbor not in visited:
                        cycle = find_cycles(graph, neighbor, visited, rec_stack, path)
                        if cycle:
                            return cycle
                    elif neighbor in rec_stack:
                        cycle_start = path.index(neighbor)
                        return path[cycle_start:] + [neighbor]
            rec_stack.remove(start)
            path.pop()
            return None

        graph = {}
        for parent, child in relationships:
            if parent not in graph:
                graph[parent] = []
            graph[parent].append(child)
        visited = set()
        circular_refs = []
        for table in graph:
            if table not in visited:
                cycle = find_cycles(graph, table, visited, set(), [])
                if cycle:
                    circular_refs.append(cycle)
        return circular_refs

    @staticmethod
    def get_extended_table_metadata_from_broadcast(broadcast_helper, database_name: str, table_name: str, schema_name: str = "dbo") -> Dict[str, Any]:
        return broadcast_helper.get_table_metadata_fast(database_name, table_name, schema_name)

    @staticmethod
    def get_referencing_tables_from_broadcast(broadcast_helper, table_name: str, database_name: str, schema_name: str = "dbo") -> List[Dict[str, Any]]:
        try:
            all_tables = broadcast_helper.get_all_tables_in_database(database_name, schema_name)
            referencing_tables = []
            for table in all_tables:
                if table == table_name:
                    continue
                try:
                    table_metadata = broadcast_helper.get_table_metadata_fast(database_name, table, schema_name)
                    for column in table_metadata["columns"]:
                        if column["foreign_key"] and column["foreign_key"]["referenced_table"] == table_name:
                            child_table_full = f"{schema_name}.{table}"
                            constrained_columns = [column["name"]]
                            referred_columns = [column["foreign_key"]["referenced_column"]]
                            existing_ref = None
                            for ref in referencing_tables:
                                if ref["child_table"] == child_table_full:
                                    existing_ref = ref
                                    break
                            if existing_ref:
                                existing_ref["constrained_columns"].append(column["name"])
                                existing_ref["referred_columns"].append(column["foreign_key"]["referenced_column"])
                            else:
                                referencing_tables.append({"child_table": child_table_full, "constrained_columns": constrained_columns, "referred_columns": referred_columns})
                except Exception as table_error:
                    logger.warning(f"Could not get metadata for table {table}: {str(table_error)}")
                    continue
            logger.info(f"Found {len(referencing_tables)} tables referencing {schema_name}.{table_name} using broadcast metadata")
            return referencing_tables
        except Exception as e:
            logger.error(f"Failed to get referencing tables for {schema_name}.{table_name} from broadcast: {str(e)}")
            raise

    @staticmethod
    def find_orphaned_records_with_broadcast(spark_session: SparkSession, broadcast_helper, table_name: str, database_name: str, schema_name: str = "dbo") -> List[Dict[str, Any]]:
        try:
            referencing_tables = UtilityFunctions.get_referencing_tables_from_broadcast(broadcast_helper, table_name, database_name, schema_name)
            client = SecretClient(vault_url=f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/", credential=DefaultAzureCredential())
            jdbc_url = f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database_name};encrypt=true;trustServerCertificate=true;"
            password_secret = client.get_secret("atldm-sql-atlassbxsql02")
            orphaned_records = []
            for ref_table in referencing_tables:
                child_table = ref_table["child_table"]
                constrained_columns = ref_table["constrained_columns"]
                referred_columns = ref_table["referred_columns"]
                join_conditions = []
                where_conditions = []
                for const_col, ref_col in zip(constrained_columns, referred_columns):
                    join_conditions.append(f"child.{const_col} = parent.{ref_col}")
                    where_conditions.append(f"parent.{ref_col} IS NULL")
                select_columns = ", ".join(constrained_columns)
                join_clause = " AND ".join(join_conditions)
                where_clause = " AND ".join(where_conditions)
                orphaned_query = f"""
                SELECT {select_columns}, COUNT(*) as orphaned_count
                FROM {child_table} child
                LEFT JOIN {schema_name}.{table_name} parent
                    ON {join_clause}
                WHERE {where_clause}
                GROUP BY {select_columns}
                """
                try:
                    orphaned_df = (
                        spark_session.read.format("jdbc")
                        .option("url", jdbc_url)
                        .option("query", orphaned_query)
                        .option("user", "atldm-sql-atlassbxsql02")
                        .option("password", password_secret.value)
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                        .load()
                    )
                    for orphan_row in orphaned_df.collect():
                        orphaned_values = {}
                        for i, col in enumerate(constrained_columns):
                            orphaned_values[col] = orphan_row[i]
                        orphaned_records.append({"child_table": child_table, "foreign_key_columns": constrained_columns, "orphaned_values": orphaned_values, "orphaned_count": orphan_row.orphaned_count})
                except Exception as query_error:
                    logger.warning(f"Failed to check orphaned records for table {child_table}: {str(query_error)}")
                    continue
            logger.info(f"Found {len(orphaned_records)} orphaned record groups for table {schema_name}.{table_name}")
            return orphaned_records
        except Exception as e:
            logger.error(f"Failed to find orphaned records for table {schema_name}.{table_name}: {str(e)}")
            raise

    # @staticmethod
    # def get_service_principal_spark_config() -> Dict[str, str]:
    #     return UtilityFunctions.get_enhanced_service_principal_spark_config()

    # @staticmethod
    # def get_hadoop_config() -> Dict[str, str]:
    #     return {
    #         "spark.hadoop.hadoop.security.group.mapping": "org.apache.hadoop.security.StaticUserWebIdentityGroupMapping",
    #         "spark.hadoop.hadoop.security.group.mapping.static.mapping.override": "true",
    #         "spark.hadoop.hadoop.security.groups.cache.secs": "0",
    #         "spark.hadoop.hadoop.security.groups.negative-cache.secs": "0",
    #         "spark.hadoop.hadoop.security.groups.shell.command.timeout": "0",
    #         "spark.hadoop.hadoop.security.groups.mapping.ldap.connection.timeout.ms": "60000",
    #         "spark.hadoop.hadoop.security.groups.mapping.ldap.read.timeout.ms": "60000",
    #         "spark.hadoop.fs.azure.skipUserGroupMetadata": "true",
    #         "spark.hadoop.fs.azurebfs.impl.disable.cache": "false",
    #         "spark.hadoop.fs.AbstractFileSystem.abfss.impl": "org.apache.hadoop.fs.azurebfs.Abfss",
    #         "spark.hadoop.fs.AbstractFileSystem.abfs.impl": "org.apache.hadoop.fs.azurebfs.Abfs",
    #         "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    #         "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true",
    #         "spark.hadoop.hadoop.security.authentication": "simple",
    #         "spark.hadoop.hadoop.security.authorization": "false",
    #     }

    # @staticmethod
    # def get_windows_compatible_spark_config() -> Dict[str, str]:
    #     config = {}
    #     is_windows = os.name == "nt" or "WINDOWS" in os.environ.get("OS", "").upper()
    #     is_azure_devops = os.getenv("AGENT_NAME") or os.getenv("BUILD_BUILDID")
    #     agent_name = os.getenv("AGENT_NAME", "")
    #     if is_windows or is_azure_devops:
    #         logger.info("Detected Windows environment or Azure DevOps - applying Windows-compatible Spark configuration")
    #         config.update(
    #             {
    #                 "spark.hadoop.hadoop.security.group.mapping": "org.apache.hadoop.security.StaticUserWebIdentityGroupMapping",
    #                 "spark.hadoop.hadoop.security.group.mapping.static.mapping.override": "true",
    #                 "spark.hadoop.hadoop.security.groups.cache.secs": "0",
    #                 "spark.hadoop.hadoop.security.groups.negative-cache.secs": "0",
    #                 "spark.hadoop.fs.azure.skipUserGroupMetadata": "true",
    #                 "spark.hadoop.fs.permissions.umask-mode": "022",
    #                 "spark.hadoop.hadoop.security.authentication": "simple",
    #                 "spark.hadoop.hadoop.security.authorization": "false",
    #                 "spark.hadoop.fs.azure.secure.mode": "false",
    #                 "spark.sql.adaptive.enabled": "false",
    #                 "spark.sql.adaptive.coalescePartitions.enabled": "false",
    #                 "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    #                 "spark.hadoop.fs.azure.oauth.provider.logging.level": "ERROR",
    #                 "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    #                 "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored": "true",
    #             }
    #         )
    #         if is_azure_devops and agent_name:
    #             config["spark.hadoop.hadoop.user.name"] = agent_name.replace("-", "_")
    #             config[f"spark.hadoop.hadoop.security.group.mapping.static.mapping.{agent_name.replace('-', '_')}"] = "users"
    #             logger.info(f"Set Hadoop user for Azure DevOps: {agent_name.replace('-', '_')}")
    #         else:
    #             config["spark.hadoop.hadoop.user.name"] = "spark_user"
    #             config["spark.hadoop.hadoop.security.group.mapping.static.mapping.spark_user"] = "users"
    #     else:
    #         logger.info("Detected Unix/Linux environment - using standard configuration")
    #     return config

    # @staticmethod
    # def get_pipeline_tenant_id() -> str:
    #     # Check for standard Azure environment variable
    #     azure_tenant_id = os.getenv("AZURE_TENANT_ID")
    #     if azure_tenant_id:
    #         logger.info("Using AZURE_TENANT_ID environment variable")
    #         return azure_tenant_id
    #     return ""

    # @staticmethod
    # def get_azure_devops_token_config() -> Dict[str, str]:
    #     storage_account = os.getenv("STORAGE_ACCOUNT", "auedatamigdevlake")
    #     access_token = os.getenv("AZURE_STORAGE_ACCESS_TOKEN")
    #     tenant_id = os.getenv("AZURE_TENANT_ID")

    #     logger.info(f"Using OAuth access token authentication for storage account: {storage_account}")
    #     final_config = {
    #         # Global auth settings
    #         "spark.hadoop.fs.azure.account.auth.type": "OAuth",
    #         "spark.hadoop.fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    #         "spark.hadoop.fs.azure.account.oauth2.client.id": "dummy-client-id",  # Required but not used
    #         "spark.hadoop.fs.azure.account.oauth2.client.secret": access_token,  # Use token as secret
    #         "spark.hadoop.fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token" if tenant_id else "https://login.microsoftonline.com/common/oauth2/token",
    #         # Storage account specific settings
    #         f"spark.hadoop.fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
    #         f"spark.hadoop.fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    #         f"spark.hadoop.fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": "dummy-client-id",
    #         f"spark.hadoop.fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": access_token,
    #         f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    #     }

    #     # Add Windows compatibility and Hadoop configs
    #     final_config.update(UtilityFunctions.get_hadoop_config())
    #     final_config.update(UtilityFunctions.get_windows_compatible_spark_config())

    #     logger.info(f"Configuration keys: {list(final_config.keys())}")
    #     return final_config

    # @staticmethod
    # def get_enhanced_service_principal_spark_config() -> Dict[str, str]:
    #     # Check if running in Azure DevOps pipeline with access token
    #     if os.getenv("AZURE_STORAGE_ACCESS_TOKEN") and os.getenv("AGENT_NAME"):
    #         logger.info("Detected Azure DevOps pipeline environment - using access token authentication")
    #         return UtilityFunctions.get_azure_devops_token_config()
    #     # Use MSI authentication with tenant ID from environment variables
    #     tenant_id = UtilityFunctions.get_pipeline_tenant_id()
    #     logger.info("Using MSI (Managed Service Identity) authentication")
    #     auth_config = {"spark.hadoop.fs.azure.account.auth.type": "OAuth", "spark.hadoop.fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"}
    #     # Add tenant ID for MSI authentication if available
    #     if tenant_id:
    #         auth_config["spark.hadoop.fs.azure.account.oauth2.msi.tenant"] = tenant_id
    #         logger.info(f"Added tenant ID for MSI authentication: {tenant_id}")
    #     else:
    #         logger.warning("No AZURE_TENANT_ID found - authentication may fail")
    #     # Merge authentication config with Windows compatibility and Hadoop configs
    #     final_config = {}
    #     final_config.update(auth_config)
    #     final_config.update(UtilityFunctions.get_hadoop_config())
    #     final_config.update(UtilityFunctions.get_windows_compatible_spark_config())
    #     # Add storage account specific configuration if available
    #     storage_account = os.getenv("STORAGE_ACCOUNT")
    #     if storage_account:
    #         # Apply the auth config to the specific storage account
    #         final_config[f"spark.hadoop.fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net"] = auth_config["spark.hadoop.fs.azure.account.auth.type"]
    #         final_config[f"spark.hadoop.fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net"] = auth_config["spark.hadoop.fs.azure.account.oauth.provider.type"]
    #         if "spark.hadoop.fs.azure.account.oauth2.msi.tenant" in auth_config:
    #             final_config[f"spark.hadoop.fs.azure.account.oauth2.msi.tenant.{storage_account}.dfs.core.windows.net"] = auth_config["spark.hadoop.fs.azure.account.oauth2.msi.tenant"]
    #         logger.info(f"Added storage account specific configuration for: {storage_account}")
    #     return final_config

    # @staticmethod
    # def get_azure_devops_spark_config() -> Dict[str, str]:
    #     config = {}
    #     if os.getenv("AGENT_NAME") or os.getenv("BUILD_BUILDID"):
    #         agent_name = os.getenv("AGENT_NAME", "spark_user").replace("-", "_")
    #         logger.info(f"Configuring Spark for Azure DevOps agent: {agent_name}")
    #         config.update(
    #             {
    #                 "spark.hadoop.hadoop.security.group.mapping": "org.apache.hadoop.security.StaticUserWebIdentityGroupMapping",
    #                 "spark.hadoop.hadoop.security.group.mapping.static.mapping.override": "true",
    #                 "spark.hadoop.hadoop.security.groups.cache.secs": "0",
    #                 "spark.hadoop.hadoop.security.groups.negative-cache.secs": "0",
    #                 "spark.hadoop.hadoop.user.name": agent_name,
    #                 f"spark.hadoop.hadoop.security.group.mapping.static.mapping.{agent_name}": "users",
    #                 "spark.hadoop.fs.azure.skipUserGroupMetadata": "true",
    #                 "spark.hadoop.hadoop.security.authentication": "simple",
    #                 "spark.hadoop.hadoop.security.authorization": "false",
    #                 "spark.sql.warehouse.dir": f"/tmp/spark-warehouse-{agent_name}",
    #                 "spark.sql.adaptive.enabled": "false",
    #                 "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    #             }
    #         )
    #         os.environ["HADOOP_USER_NAME"] = agent_name
    #         os.environ["USER"] = agent_name
    #         logger.info(f"Set environment variables: HADOOP_USER_NAME={agent_name}, USER={agent_name}")
    #     return config

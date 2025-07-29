import os
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.broadcast import Broadcast
from loguru import logger
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from src.Utilities import UtilityFunctions as utils


class MetadataBroadcastManager:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.broadcast_cache: Dict[str, Broadcast] = {}
        self.metadata_cache: Dict[str, List[Dict]] = {}

    def get_database_metadata_broadcast(self, database_name: str, schema_name: str = "dbo") -> Broadcast:
        cache_key = f"{database_name}_{schema_name}"
        if cache_key not in self.broadcast_cache:
            logger.info(f"Creating broadcast for database {database_name} metadata")
            metadata_df = self._get_all_tables_metadata(database_name, schema_name)
            # Convert DataFrame to list of dictionaries for broadcasting
            metadata_rows = [row.asDict() for row in metadata_df.collect()]
            self.metadata_cache[cache_key] = metadata_rows
            self.broadcast_cache[cache_key] = self.spark_session.sparkContext.broadcast(metadata_rows)
            logger.info(f"Broadcast created for {database_name} with {len(metadata_rows)} metadata records")
        return self.broadcast_cache[cache_key]

    def get_table_metadata_from_broadcast(self, database_name: str, table_name: str, schema_name: str = "dbo") -> List[Dict]:
        broadcast_var = self.get_database_metadata_broadcast(database_name, schema_name)
        metadata_rows = broadcast_var.value
        # Filter the rows in Python instead of using DataFrame operations
        filtered_rows = [row for row in metadata_rows if row["table_name"] == table_name and row["database_name"] == database_name and row["schema_name"] == schema_name]
        return filtered_rows

    def get_table_metadata_dict_from_broadcast(self, database_name: str, table_name: str, schema_name: str = "dbo") -> Dict:
        filtered_rows = self.get_table_metadata_from_broadcast(database_name, table_name, schema_name)
        if len(filtered_rows) == 0:
            raise ValueError(f"Table '{schema_name}.{table_name}' not found in database '{database_name}' metadata broadcast")
        columns_list = []
        for row in filtered_rows:
            column_metadata = {
                "name": row["column_name"],
                "data_type": row["data_type"],
                "max_length": row["max_length"],
                "precision": row["precision"],
                "scale": row["scale"],
                "is_nullable": row["is_nullable"],
                "is_identity": row["is_identity"],
                "default_value": row["default_object_id"],
                "computed_column_definition": row["definition"],
                "is_primary_key": row["is_primary_key"],
                "is_unique_constraint": row["is_unique_constraint"],
                "foreign_key": {"name": row["foreign_key_name"], "referenced_table": row["referenced_table_name"], "referenced_column": row["referenced_column_name"]} if row["foreign_key_name"] else None,
            }
            columns_list.append(column_metadata)
        return {"schema": schema_name, "table_name": table_name, "database_name": database_name, "columns": columns_list}

    def _get_all_tables_metadata(self, database_name: str, schema_name: str = "dbo") -> DataFrame:
        try:
            client = SecretClient(vault_url=f"https://{os.getenv('KEY_VAULT_NAME')}.vault.azure.net/", credential=DefaultAzureCredential())
            password_secret = client.get_secret("atldm-sql-atlassbxsql02")
            jdbc_url = f"jdbc:sqlserver://{os.getenv('SOURCE_SERVER')};databaseName={database_name};encrypt=true;trustServerCertificate=true;"
            connection_properties = utils.get_connection_properties()
            connection_properties["user"] = "atldm-sql-atlassbxsql02"
            connection_properties["password"] = password_secret.value
            tables_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.tables").options(**connection_properties).load()
            tables_df = tables_df.select("object_id", "name", "schema_id")
            tables_df = tables_df.withColumnRenamed("name", "table_name").withColumnRenamed("object_id", "table_object_id").withColumnRenamed("schema_id", "table_schema_id")
            schemas_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.schemas").options(**connection_properties).load()
            schemas_df = schemas_df.select("name", "schema_id")
            schemas_df = schemas_df.withColumnRenamed("name", "schema_name").withColumnRenamed("schema_id", "schema_schema_id")
            columns_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.columns").options(**connection_properties).load()
            columns_df = columns_df.select("object_id", "column_id", "name", "user_type_id", "max_length", "precision", "scale", "is_nullable", "is_identity", "default_object_id")
            columns_df = columns_df.withColumnRenamed("name", "column_name").withColumnRenamed("object_id", "column_object_id").withColumnRenamed("column_id", "col_id")
            types_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.types").options(**connection_properties).load()
            types_df = types_df.select("user_type_id", "name")
            types_df = types_df.withColumnRenamed("name", "data_type").withColumnRenamed("user_type_id", "type_user_type_id")
            computed_columns_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.computed_columns").options(**connection_properties).load()
            computed_columns_df = computed_columns_df.select("object_id", "column_id", "definition")
            computed_columns_df = computed_columns_df.withColumnRenamed("object_id", "cc_object_id").withColumnRenamed("column_id", "cc_column_id")
            index_columns_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.index_columns").options(**connection_properties).load()
            index_columns_df = index_columns_df.select("object_id", "index_id", "column_id")
            index_columns_df = index_columns_df.withColumnRenamed("object_id", "ic_object_id").withColumnRenamed("column_id", "ic_column_id")
            indexes_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.indexes").options(**connection_properties).load()
            indexes_df = indexes_df.select("object_id", "index_id", "is_primary_key", "is_unique_constraint")
            indexes_df = indexes_df.withColumnRenamed("object_id", "idx_object_id").withColumnRenamed("index_id", "idx_index_id")
            foreign_key_columns_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.foreign_key_columns").options(**connection_properties).load()
            foreign_key_columns_df = foreign_key_columns_df.select("constraint_object_id", "parent_object_id", "parent_column_id", "referenced_object_id", "referenced_column_id")
            foreign_key_columns_df = (
                foreign_key_columns_df.withColumnRenamed("constraint_object_id", "fkc_constraint_object_id").withColumnRenamed("parent_object_id", "fkc_parent_object_id").withColumnRenamed("parent_column_id", "fkc_parent_column_id")
            )
            foreign_keys_df = self.spark_session.read.format("jdbc").option("url", jdbc_url).option("dbtable", "sys.foreign_keys").options(**connection_properties).load()
            foreign_keys_df = foreign_keys_df.select("object_id", "name")
            foreign_keys_df = foreign_keys_df.withColumnRenamed("name", "foreign_key_name").withColumnRenamed("object_id", "fk_object_id")
            referenced_tables_df = (
                tables_df.select("table_object_id", "table_name", "table_schema_id")
                .withColumnRenamed("table_object_id", "ref_table_object_id")
                .withColumnRenamed("table_name", "referenced_table_name")
                .withColumnRenamed("table_schema_id", "ref_table_schema_id")
            )
            referenced_columns_df = (
                columns_df.select("column_object_id", "col_id", "column_name")
                .withColumnRenamed("column_object_id", "ref_column_object_id")
                .withColumnRenamed("col_id", "ref_col_id")
                .withColumnRenamed("column_name", "referenced_column_name")
            )
            index_constraints_df = index_columns_df.join(indexes_df, (index_columns_df.ic_object_id == indexes_df.idx_object_id) & (index_columns_df.index_id == indexes_df.idx_index_id), "inner").select(
                "ic_object_id", "ic_column_id", "is_primary_key", "is_unique_constraint"
            )
            main_df = (
                tables_df.join(schemas_df, tables_df.table_schema_id == schemas_df.schema_schema_id, "inner")
                .join(columns_df, tables_df.table_object_id == columns_df.column_object_id, "inner")
                .join(types_df, columns_df.user_type_id == types_df.type_user_type_id, "inner")
            )
            fk_with_references = foreign_key_columns_df.join(referenced_tables_df, foreign_key_columns_df.referenced_object_id == referenced_tables_df.ref_table_object_id, "left").join(
                referenced_columns_df, (foreign_key_columns_df.referenced_object_id == referenced_columns_df.ref_column_object_id) & (foreign_key_columns_df.referenced_column_id == referenced_columns_df.ref_col_id), "left"
            )
            from pyspark.sql.functions import lit, when, col

            result_df = (
                main_df.join(computed_columns_df, (main_df.column_object_id == computed_columns_df.cc_object_id) & (main_df.col_id == computed_columns_df.cc_column_id), "left")
                .join(index_constraints_df, (main_df.column_object_id == index_constraints_df.ic_object_id) & (main_df.col_id == index_constraints_df.ic_column_id), "left")
                .join(fk_with_references, (main_df.column_object_id == fk_with_references.fkc_parent_object_id) & (main_df.col_id == fk_with_references.fkc_parent_column_id), "left")
                .join(foreign_keys_df, fk_with_references.fkc_constraint_object_id == foreign_keys_df.fk_object_id, "left")
                .filter(main_df.schema_name == schema_name)
                .filter(~main_df.table_name.contains("DO_NOT"))
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
                    "referenced_table_name",
                    "referenced_column_name",
                )
                .withColumn("database_name", lit(database_name))
                .withColumn("is_primary_key", when(col("is_primary_key").isNull(), lit(False)).otherwise(col("is_primary_key")))
                .withColumn("is_unique_constraint", when(col("is_unique_constraint").isNull(), lit(False)).otherwise(col("is_unique_constraint")))
            )
            logger.info(f"Retrieved complete metadata for database {database_name}: {result_df.count()} total column records")
            return result_df
        except Exception as e:
            logger.error(f"Failed to get complete metadata for database {database_name}: {str(e)}")
            raise

    def clear_cache(self, database_name: str = None, schema_name: str = "dbo"):
        if database_name:
            cache_key = f"{database_name}_{schema_name}"
            if cache_key in self.broadcast_cache:
                self.broadcast_cache[cache_key].unpersist()
                del self.broadcast_cache[cache_key]
                del self.metadata_cache[cache_key]
                logger.info(f"Cleared broadcast cache for {database_name}")
        else:
            for broadcast_var in self.broadcast_cache.values():
                broadcast_var.unpersist()
            self.broadcast_cache.clear()
            self.metadata_cache.clear()
            logger.info("Cleared all broadcast caches")

    def __del__(self):
        self.clear_cache()

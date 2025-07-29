import pytest
import time
from loguru import logger
from chispa import dataframe_comparer as dc
from pyspark.sql.functions import length, max as spark_max, min as spark_min
from icecream import ic
from pytest_check import check

ic.configureOutput(includeContext=True, contextAbsPath=True)


class BaseDataValidationTest:
    # def test_data_diagnosis(self, data_loader, spark_session, table_name, database_name, ):
    def test_data_diagnosis(
        self,
        data_loader,
        spark_session,
        table_name,
        database_name,
    ):
        start_time = time.time()
        metadata_lookup_time = time.time() - start_time
        loader = data_loader(spark_session, table_name, database_name)
        source_count = loader.source_sdf.count()
        target_count = loader.target_sdf.count()
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        source_only = source_columns - target_columns
        target_only = target_columns - source_columns
        logger.info(f"=== DATA DIAGNOSIS FOR {table_name} (BROADCAST) ===")
        logger.info(f"Broadcast metadata lookup time: {metadata_lookup_time:.3f}s")
        logger.info(f"Source row count: {source_count}")
        logger.info(f"Target row count: {target_count}")
        logger.info(f"Row count difference: {abs(source_count - target_count)}")
        logger.info(f"Source columns: {len(source_columns)}")
        logger.info(f"Target columns: {len(target_columns)}")
        logger.info(f"Common columns: {len(common_columns)}")
        logger.info(f"Source-only columns: {sorted(source_only)}")
        logger.info(f"Target-only columns: {sorted(target_only)}")
        if common_columns:
            sample_column = list(common_columns)[0]
            source_sample = loader.source_sdf.select(sample_column).limit(5).collect()
            target_sample = loader.target_sdf.select(sample_column).limit(5).collect()
            ic(source_sample)
            ic(target_sample)
            logger.info(f"Source sample ({sample_column}): {[row[0] for row in source_sample]}")
            logger.info(f"Target sample ({sample_column}): {[row[0] for row in target_sample]}")
        assert True, "Diagnostic test completed"

    def test_row_counts(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_count = loader.source_sdf.count()
        target_count = loader.target_sdf.count()
        check.equal(source_count, target_count, f"Row count mismatch: source={source_count}, target={target_count}")
        logger.success("test_row_counts - Finished")

    def test_column_counts(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_number_of_columnms = len(loader.source_sdf.columns)
        target_number_of_columnms = len(loader.target_sdf.columns)
        check.equal(source_number_of_columnms, target_number_of_columnms, f"Column count mismatch: source={source_number_of_columnms}, target={target_number_of_columnms}")
        logger.success("test_column_counts - Finished")

    def test_dataframe_general_equality(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = list(source_columns.intersection(target_columns))
        if len(common_columns) == 0:
            logger.warning("No common columns found for dataframe equality comparison")
            return
        source_subset = loader.source_sdf.select(*common_columns)
        target_subset = loader.target_sdf.select(*common_columns)
        check.equal(dc.assert_df_equality(source_subset, target_subset, ignore_nullable=True), True, "Dataframes are not equal")
        logger.success("test_dataframe_general_equality - Finished")

    def test_dataframe_schema_adherence(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = list(source_columns.intersection(target_columns))
        if len(common_columns) == 0:
            logger.warning("No common columns found for schema adherence comparison")
            return
        source_subset = loader.source_sdf.select(*common_columns)
        target_subset = loader.target_sdf.select(*common_columns)
        check.equal(dc.assert_df_equality(source_subset, target_subset, ignore_nullable=False), True, "Dataframes do not adhere to the same schema")
        logger.success("test_dataframe_schema_adherence - Finished")

    def test_dataframe_equality_ignore_row_order(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = list(source_columns.intersection(target_columns))
        if len(common_columns) == 0:
            logger.warning("No common columns found for row order comparison")
            return
        source_subset = loader.source_sdf.select(*common_columns)
        target_subset = loader.target_sdf.select(*common_columns)
        check.equal(dc.assert_df_equality(source_subset, target_subset, ignore_nullable=False, ignore_row_order=True), True, "Dataframes are not equal when ignoring row order")
        logger.success("test_dataframe_equality_ignore_row_order - Finished")

    def test_column_match(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        logger.info(f"Source columns: {sorted(source_columns)}")
        logger.info(f"Target columns: {sorted(target_columns)}")
        logger.info(f"Common columns: {sorted(common_columns)}")
        check.equal(len(common_columns), 0, "No common columns found between source and target")
        logger.success("test_column_match - Finished")

    def test_schema_data_types(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        logger.info(f"Comparing data types for {len(common_columns)} common columns")
        source_schema = {field.name: str(field.dataType) for field in loader.source_sdf.schema.fields if field.name in common_columns}
        target_schema = {field.name: str(field.dataType) for field in loader.target_sdf.schema.fields if field.name in common_columns}
        for column in common_columns:
            check.equal(source_schema[column], target_schema[column], f"Data type mismatch for column {column}: source={source_schema[column]}, target={target_schema[column]}")
        logger.success("test_schema_data_types - Finished")

    def test_null_count_per_column(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        logger.info(f"Source columns: {len(source_columns)}, Target columns: {len(target_columns)}, Common columns: {len(common_columns)}")
        for column in common_columns:
            source_null_count = loader.source_sdf.filter(loader.source_sdf[column].isNull()).count()
            target_null_count = loader.target_sdf.filter(loader.target_sdf[column].isNull()).count()
            check.equal(source_null_count, target_null_count, f"Null count mismatch for column {column}: source={source_null_count}, target={target_null_count}")
        logger.success("test_null_count_per_column - Finished")

    def test_duplicate_row_count(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = list(source_columns.intersection(target_columns))
        if len(common_columns) == 0:
            logger.warning("No common columns found for duplicate row comparison")
            return
        source_subset = loader.source_sdf.select(*common_columns)
        target_subset = loader.target_sdf.select(*common_columns)
        source_distinct_count = source_subset.distinct().count()
        target_distinct_count = target_subset.distinct().count()
        source_total_count = source_subset.count()
        target_total_count = target_subset.count()
        source_duplicate_count = source_total_count - source_distinct_count
        target_duplicate_count = target_total_count - target_distinct_count
        check.equal(source_duplicate_count, target_duplicate_count, f"Duplicate count mismatch: source={source_duplicate_count}, target={target_duplicate_count}")
        logger.success("test_duplicate_row_count - Finished")

    def test_column_nullable_properties(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        logger.info(f"Comparing nullable properties for {len(common_columns)} common columns")
        source_nullable = {field.name: field.nullable for field in loader.source_sdf.schema.fields if field.name in common_columns}
        target_nullable = {field.name: field.nullable for field in loader.target_sdf.schema.fields if field.name in common_columns}
        for column in common_columns:
            check.equal(source_nullable[column] == target_nullable[column], f"Nullable property mismatch for column {column}: source={source_nullable[column]}, target={target_nullable[column]}")
        logger.success("test_column_nullable_properties - Finished")

    def test_numeric_column_ranges(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        numeric_columns = [field.name for field in loader.source_sdf.schema.fields if str(field.dataType) in ["IntegerType", "LongType", "DoubleType", "FloatType", "DecimalType"] and field.name in common_columns]
        logger.info(f"Comparing ranges for {len(numeric_columns)} common numeric columns")
        for column in numeric_columns:
            source_stats = loader.source_sdf.select(column).describe().collect()
            target_stats = loader.target_sdf.select(column).describe().collect()
            source_min = float(source_stats[3][1]) if source_stats[3][1] else None
            target_min = float(target_stats[3][1]) if target_stats[3][1] else None
            source_max = float(source_stats[4][1]) if source_stats[4][1] else None
            target_max = float(target_stats[4][1]) if target_stats[4][1] else None
            check.equal(source_min, target_min, f"Min value mismatch for column {column}: source={source_min}, target={target_min}")
            check.equal(source_max, target_max, f"Max value mismatch for column {column}: source={source_max}, target={target_max}")
        logger.success("test_numeric_column_ranges - Finished")

    def test_string_column_lengths(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        string_columns = [field.name for field in loader.source_sdf.schema.fields if str(field.dataType) == "StringType" and field.name in common_columns]
        logger.info(f"Comparing string lengths for {len(string_columns)} common string columns")
        for column in string_columns:
            source_max_length = loader.source_sdf.select(spark_max(length(column)).alias("max_length")).collect()[0]["max_length"]
            target_max_length = loader.target_sdf.select(spark_max(length(column)).alias("max_length")).collect()[0]["max_length"]
            source_min_length = loader.source_sdf.select(spark_min(length(column)).alias("min_length")).collect()[0]["min_length"]
            target_min_length = loader.target_sdf.select(spark_min(length(column)).alias("min_length")).collect()[0]["min_length"]
            check.equal(source_max_length, target_max_length, f"Max length mismatch for column {column}: source={source_max_length}, target={target_max_length}")
            check.equal(source_min_length, target_min_length, f"Min length mismatch for column {column}: source={source_min_length}, target={target_min_length}")
        logger.success("test_string_column_lengths - Finished")

    def test_date_column_ranges(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        date_columns = [field.name for field in loader.source_sdf.schema.fields if str(field.dataType) in ["DateType", "TimestampType"] and field.name in common_columns]
        logger.info(f"Comparing date ranges for {len(date_columns)} common date columns")
        for column in date_columns:
            source_min_date = loader.source_sdf.select(spark_min(column).alias("min_date")).collect()[0]["min_date"]
            target_min_date = loader.target_sdf.select(spark_min(column).alias("min_date")).collect()[0]["min_date"]
            source_max_date = loader.source_sdf.select(spark_max(column).alias("max_date")).collect()[0]["max_date"]
            target_max_date = loader.target_sdf.select(spark_max(column).alias("max_date")).collect()[0]["max_date"]
            check.equal(source_min_date, target_min_date, f"Min date mismatch for column {column}: source={source_min_date}, target={target_min_date}")
            check.equal(source_max_date, target_max_date, f"Max date mismatch for column {column}: source={source_max_date}, target={target_max_date}")
        logger.success("test_date_column_ranges - Finished")

    def test_unique_value_counts(self, data_loader, spark_session, table_name, database_name):
        loader = data_loader(spark_session, table_name, database_name)
        source_columns = set(loader.source_sdf.columns)
        target_columns = set(loader.target_sdf.columns)
        common_columns = source_columns.intersection(target_columns)
        if not common_columns:
            pytest.skip("No common columns found between source and target")
        for column in common_columns:
            source_unique_count = loader.source_sdf.select(column).distinct().count()
            target_unique_count = loader.target_sdf.select(column).distinct().count()
            check.equal(source_unique_count, target_unique_count, f"Unique value count mismatch for column {column}: source={source_unique_count}, target={target_unique_count}")

    # def test_table_metadata_retrieval(self, data_loader, spark_session, table_name, database_name, broadcast_metadata_helper):
    #     start_time = time.time()
    #     metadata = broadcast_metadata_helper.get_table_metadata_fast(database_name, table_name)
    #     metadata_lookup_time = time.time() - start_time
    #     logger.info(f"Broadcast metadata lookup time: {metadata_lookup_time:.3f}s")
    #     assert metadata is not None, "Table metadata should not be None"
    #     assert metadata["table_name"] == table_name, f"Expected table name {table_name}, got {metadata['table_name']}"
    #     assert metadata["database_name"] == database_name, f"Expected database name {database_name}, got {metadata['database_name']}"
    #     assert len(metadata["columns"]) > 0, "Table should have at least one column"
    #     logger.info(f"Retrieved metadata for {metadata['table_name']}: {len(metadata['columns'])} columns")
    #     for col in metadata["columns"][:3]:
    #         logger.info(f"Column: {col['name']}, Type: {col['data_type']}, Nullable: {col['is_nullable']}")

    # def test_schema_comparison(self, data_loader, spark_session, table_name, database_name, broadcast_metadata_helper):
    #     try:
    #         start_time = time.time()
    #         source_metadata = broadcast_metadata_helper.get_table_metadata_fast(database_name, table_name)
    #         target_metadata = broadcast_metadata_helper.get_table_metadata_fast(database_name, table_name)
    #         metadata_lookup_time = time.time() - start_time
    #         logger.info(f"Broadcast metadata lookup time: {metadata_lookup_time:.3f}s")
    #         source_columns = {col["name"]: col for col in source_metadata["columns"]}
    #         target_columns = {col["name"]: col for col in target_metadata["columns"]}
    #         common_columns = set(source_columns.keys()).intersection(set(target_columns.keys()))
    #         source_only_columns = list(set(source_columns.keys()) - set(target_columns.keys()))
    #         target_only_columns = list(set(target_columns.keys()) - set(source_columns.keys()))
    #         comparison = {
    #             "table_name": table_name,
    #             "common_columns_count": len(common_columns),
    #             "source_columns_count": len(source_columns),
    #             "target_columns_count": len(target_columns),
    #             "source_only_columns": source_only_columns,
    #             "target_only_columns": target_only_columns,
    #         }
    #         assert comparison is not None, "Schema comparison should not be None"
    #         assert comparison["table_name"] == table_name, f"Expected table name {table_name}, got {comparison['table_name']}"
    #         logger.info(f"Schema comparison results: {comparison['common_columns_count']} common columns")
    #         if comparison["source_only_columns"]:
    #             logger.warning(f"Source-only columns: {comparison['source_only_columns']}")
    #         if comparison["target_only_columns"]:
    #             logger.warning(f"Target-only columns: {comparison['target_only_columns']}")
    #         if comparison["schema_differences"]:
    #             logger.warning(f"Schema differences found: {len(comparison['schema_differences'])}")
    #     except Exception as e:
    #         pytest.skip(f"Schema comparison test skipped due to environment constraints: {str(e)}")

    # def test_primary_key_uniqueness(self, data_loader, spark_session, table_name, database_name, broadcast_metadata_helper):
    #     try:
    #         start_time = time.time()
    #         metadata = broadcast_metadata_helper.get_table_metadata_fast(database_name, table_name)
    #         metadata_lookup_time = time.time() - start_time
    #         logger.info(f"Broadcast metadata lookup time: {metadata_lookup_time:.3f}s")
    #         pk_columns = [col["name"] for col in metadata["columns"] if col["is_primary_key"]]
    #         if not pk_columns:
    #             logger.warning(f"No primary key columns found in metadata for table {table_name}")
    #             pytest.skip("No primary key columns identified from database metadata")
    #         loader = data_loader(spark_session, table_name, database_name)
    #         source_columns = set(loader.source_sdf.columns)
    #         target_columns = set(loader.target_sdf.columns)
    #         available_pk_columns = [col for col in pk_columns if col in source_columns and col in target_columns]
    #         if not available_pk_columns:
    #             logger.warning(f"Primary key columns {pk_columns} not found in both source and target dataframes")
    #             pytest.skip("Primary key columns not available in both datasets")
    #         logger.info(f"Testing primary key uniqueness for identified PK columns: {available_pk_columns}")
    #         source_pk_count = loader.source_sdf.select(*available_pk_columns).distinct().count()
    #         target_pk_count = loader.target_sdf.select(*available_pk_columns).distinct().count()
    #         source_total = loader.source_sdf.count()
    #         target_total = loader.target_sdf.count()
    #         assert source_pk_count == source_total, f"Primary key not unique in source: distinct={source_pk_count}, total={source_total} for columns {available_pk_columns}"
    #         assert target_pk_count == target_total, f"Primary key not unique in target: distinct={target_pk_count}, total={target_total} for columns {available_pk_columns}"
    #         logger.success("test_primary_key_uniqueness - Finished")
    #     except Exception as e:
    #         logger.warning(f"Primary key uniqueness test failed due to metadata retrieval: {str(e)}")
    #         pytest.skip(f"Primary key uniqueness test skipped: {str(e)}")

    # def test_foreign_key_integrity(self, data_loader, spark_session, table_name, database_name, broadcast_metadata_helper):
    #     try:
    #         start_time = time.time()
    #         metadata = broadcast_metadata_helper.get_table_metadata_fast(database_name, table_name)
    #         metadata_lookup_time = time.time() - start_time
    #         logger.info(f"Broadcast metadata lookup time: {metadata_lookup_time:.3f}s")
    #         fk_columns = [col for col in metadata["columns"] if col["foreign_key"] is not None]
    #         if not fk_columns:
    #             logger.info(f"No foreign key columns found in metadata for table {table_name}")
    #             pytest.skip("No foreign key columns identified from database metadata")
    #         loader = data_loader(spark_session, table_name, database_name)
    #         source_columns = set(loader.source_sdf.columns)
    #         target_columns = set(loader.target_sdf.columns)
    #         for fk_col in fk_columns:
    #             col_name = fk_col["name"]
    #             if col_name not in source_columns or col_name not in target_columns:
    #                 logger.warning(f"Foreign key column {col_name} not found in both source and target")
    #                 continue
    #             logger.info(f"Testing foreign key integrity for column {col_name} -> {fk_col['foreign_key']['referenced_table']}.{fk_col['foreign_key']['referenced_column']}")
    #             source_null_count = loader.source_sdf.filter(loader.source_sdf[col_name].isNull()).count()
    #             target_null_count = loader.target_sdf.filter(loader.target_sdf[col_name].isNull()).count()
    #             assert source_null_count == target_null_count, f"Foreign key null count mismatch for {col_name}: source={source_null_count}, target={target_null_count}"
    #             source_distinct_count = loader.source_sdf.select(col_name).distinct().count()
    #             target_distinct_count = loader.target_sdf.select(col_name).distinct().count()
    #             assert source_distinct_count == target_distinct_count, f"Foreign key distinct count mismatch for {col_name}: source={source_distinct_count}, target={target_distinct_count}"
    #         logger.success("test_foreign_key_integrity - Finished")
    #     except Exception as e:
    #         logger.warning(f"Foreign key integrity test failed due to metadata retrieval: {str(e)}")
    #         pytest.skip(f"Foreign key integrity test skipped: {str(e)}")

    # def test_metadata_driven_column_validation(self, data_loader, spark_session, table_name, database_name, broadcast_metadata_helper):
    #     try:
    #         start_time = time.time()
    #         metadata = broadcast_metadata_helper.get_table_metadata_fast(database_name, table_name)
    #         metadata_lookup_time = time.time() - start_time
    #         logger.info(f"Broadcast metadata lookup time: {metadata_lookup_time:.3f}s")
    #         loader = data_loader(spark_session, table_name, database_name)
    #         source_columns = set(loader.source_sdf.columns)
    #         target_columns = set(loader.target_sdf.columns)
    #         identity_columns = [col["name"] for col in metadata["columns"] if col["is_identity"]]
    #         nullable_columns = [col["name"] for col in metadata["columns"] if col["is_nullable"]]
    #         not_nullable_columns = [col["name"] for col in metadata["columns"] if not col["is_nullable"]]
    #         logger.info(f"Found {len(identity_columns)} identity columns, {len(nullable_columns)} nullable columns, {len(not_nullable_columns)} not-null columns")
    #         for col_name in identity_columns:
    #             if col_name in source_columns and col_name in target_columns:
    #                 logger.info(f"Validating identity column: {col_name}")
    #                 source_null_count = loader.source_sdf.filter(loader.source_sdf[col_name].isNull()).count()
    #                 target_null_count = loader.target_sdf.filter(loader.target_sdf[col_name].isNull()).count()
    #                 assert source_null_count == 0, f"Identity column {col_name} should not have nulls in source: found {source_null_count}"
    #                 assert target_null_count == 0, f"Identity column {col_name} should not have nulls in target: found {target_null_count}"
    #         for col_name in not_nullable_columns:
    #             if col_name in source_columns and col_name in target_columns:
    #                 source_null_count = loader.source_sdf.filter(loader.source_sdf[col_name].isNull()).count()
    #                 target_null_count = loader.target_sdf.filter(loader.target_sdf[col_name].isNull()).count()
    #                 assert source_null_count == target_null_count, f"Not-null column {col_name} null count mismatch: source={source_null_count}, target={target_null_count}"
    #         logger.success("test_metadata_driven_column_validation - Finished")
    #     except Exception as e:
    #         logger.warning(f"Metadata-driven column validation failed: {str(e)}")
    #         pytest.skip(f"Metadata-driven validation test skipped: {str(e)}")

import pyspark
from delta import *
from delta_migrations import DeltaMigrationHelper
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

print("Running Template example 0000_migration.py")

def spark_setup():
    builder = (
        pyspark.sql.SparkSession.builder
        .master("local[1]")
        .appName("delta_migrations")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

input = {
            'table_name': 'example1',
            'path': '/tmp/bronze/delta/example1', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }

spark = spark_setup()
delta_migration_helper = DeltaMigrationHelper(spark)
delta_migration_helper.modify_delta_table(input)


print("Completed Template example 0000_migration.py")

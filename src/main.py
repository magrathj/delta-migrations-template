
import pyspark
from delta import *
from delta_migrations import DeltaMigrationRunner 

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

if __name__ == "__main__":
    delta_migration_history_path = '/tmp/history/'
    spark = spark_setup()
    DeltaMigrationRunner(spark, delta_migration_history_path).main()

from delta_migrations import DeltaMigrationHelper

print("Run Template example 0000_migration.py")

input = {
            'table_name': 'example1',
            'path': '/tmp/bronze/delta/example1', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
delta_migration_helper = DeltaMigrationHelper(spark)
delta_migration_helper.modify_delta_table(input)

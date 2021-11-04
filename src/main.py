from delta_migrations import main 

if __name__ == "__main__":
    delta_migration_history_path = '/tmp/history/'
    main(spark, delta_migration_history_path)
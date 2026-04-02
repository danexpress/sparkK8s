from pyspark.sql import SparkSession


def main():
    print("=" * 60)
    print("   SPARK SIMPLE COUNTER JOB")
    print("=" * 60)

    # Create Spark session
    spark = SparkSession.builder.appName("SimpleCounter").getOrCreate()

    sc - spark.sparkContext
    sc.setLogLevel("Warn")

    print(f"\n[INFO] Spark Version:: {spark.version}")
    print(f"\n[INFO] Application ID: {spark.applicationId}")
    print(f"[INFO] MASTER: {sc.master}")

    start_time = time.time()


if __name__ == "__main__":
    main()

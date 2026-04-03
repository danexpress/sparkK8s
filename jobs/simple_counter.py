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
    num_elements = 1_000_000
    num_partitions = 10

    print(
        f"\n[STEP 1] Creating RDD with {num_elements} elements accross {num_partitions} partitions..."
    )
    numbers_rdd = sc.parallelize(range(1, num_elements + 1), num_partitions)

    print("[STEP 2] Computing statistics in parallel...")
    count = numbers_rdd.count()
    print(f"  - Count: {count:,}")

    total_sum = numbers_rdd.sum()
    print(f"  - Sum: {total_sum:,}")

    average = total_sum / count
    print(f"  - Average: {average:,.2f}")

    min_value = numbers_rdd.min()
    max_value = numbers_rdd.max()
    print(f"  - Min: {min_value:,}")
    print(f"  - Max: {max_value:,}")

    print("\n[STEP 3] Counting even and odd numbers...")
    even_count = numbers_rdd.filter(lambda x: x % 2 == 0).count()
    odd_count = numbers_rdd.filter(lambda x: x % 2 != 0).count()
    print(f"  - Even Count: {even_count:,}")
    print(f"  - Odd Count: {odd_count:,}")

    # DataFrame example
    print("\n[STEP 4] Creating DataFrame and performing aggregations...")
    df = spark.createDataFrame(
        [(i, i * 2, "even" if i % 2 == 0 else "odd") for i in range(1, 101)],
        ["number", "double", "type"],
    )

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"[INFO] Elapsed Time: {elapsed_time:.2f} seconds")

    print("\n[INFO] Sample Dataframe:")
    df.show(5)

    print(' Aggregation by "type":')
    df.groupBy("type").agg({"number": "sum", "double": "avg"}).show()

    elapsed_time = time.time() - start_time

    print(f"[INFO] Job complete in : {elapsed_time:.2f} seconds")

    print("=" * 60)
    print("   SPARK SIMPLE COUNTER JOB COMPLETE")
    print(f"[INFO] Job completed in {elapsed_time:.2f} seconds")

    spark.stop()


if __name__ == "__main__":
    main()

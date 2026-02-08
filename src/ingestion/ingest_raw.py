import os
import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from jobspy import scrape_jobs
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from databricks.sdk.runtime import dbutils

def get_mongo_uri():
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return dbutils.secrets.get("mongo-secrets", "mongo-uri")
    else:
        from dotenv import load_dotenv
        load_dotenv() 
        return os.getenv("MONGO_URI")

def main():
    # Initialize the Spark Session (The "Engine")
    spark = SparkSession.builder.appName("McWics_Bronze_Ingestion").getOrCreate()
    
    # Setup MongoDB Connection
    uri = get_mongo_uri()
    client = MongoClient(uri, server_api=ServerApi("1"))
    db = client["McWics"]
    collection = db["bronze_jobspy"]

    # Categorized Boolean Batches (Includes your full list)
    search_batches = [
        "(Software OR Developer OR Full-stack OR Backend OR Frontend) AND (Intern OR Internship OR Co-op)",
        "(Data OR AI OR 'Machine Learning' OR 'Data Science' OR Analytics) AND (Intern OR Internship OR Co-op)",
        "(Cloud OR DevOps OR SRE OR Infrastructure OR Platform) AND (Intern OR Internship OR Co-op)",
        "(Cybersecurity OR 'Information Security' OR IT OR Network) AND (Intern OR Internship OR Co-op)",
        "(Product OR UX OR Design OR 'Product Management') AND (Intern OR Internship OR Co-op)",
        "(QA OR Testing OR Hardware OR Embedded) AND (Intern OR Internship OR Co-op)",
        "(Technology OR Engineering OR 'Computer Science' OR STEM) AND (Intern OR Internship OR Summer)"
    ]

    all_pandas_df = []

    # Ingestion Loop
    print(f"Starting Ingestion for {len(search_batches)} categories...")
    for query in search_batches:
        try:
            # JobSpy returns Pandas
            jobs_pd = scrape_jobs(
                site_name=["linkedin", "indeed"],
                search_term=query,
                location="Montreal, QC",
                results_wanted=30, # Moderate volume for broad coverage
                hours_old=72
            )
            if not jobs_pd.empty:
                all_pandas_df.append(jobs_pd)
        except Exception as e:
            print(f"Error scraping batch {query[:20]}: {e}")

    if all_pandas_df:
        # Convert to PySpark for Professional Processing
        # We combine Pandas results once and hand off to Spark immediately
        raw_pd_combined = pd.concat(all_pandas_df).astype(str)
        spark_df = spark.createDataFrame(raw_pd_combined)

        # Spark-Native Deduplication
        # This is more efficient for large datasets than Pandas drop_duplicates
        unique_spark_df = spark_df.dropDuplicates(["job_url"])

        # Add Engineering Metadata
        final_df = unique_spark_df.withColumn("ingested_at", F.current_timestamp()) \
                                  .withColumn("source_system", F.lit("jobspy_scraper")) \
                                  .withColumn("extraction_date", F.lit(datetime.datetime.now().strftime("%Y-%m-%d")))

        # Final Export to MongoDB
        # Using .collect() to convert back for the PyMongo driver (standard for smaller student datasets)
        # Note: For multi-million rows, you'd use the Spark-Mongo Connector
        final_data = [row.asDict() for row in final_df.collect()]
        
        if final_data:
            collection.insert_many(final_data)
            print(f"SUCCESS: {len(final_data)} unique jobs added to Bronze via PySpark.")
    else:
        print("No jobs found during this run.")

if __name__ == "__main__":
    main()
from pyspark.sql import functions as F
from src.ingestion.ingest_raw import main as run_bronze # Importing your existing Bronze logic

def silver_processing():
    #Trigger the Bronze Ingestion
    bronze_df = run_bronze()
    
    if bronze_df is None:
        print("No new data found in Bronze to process.")
        return None

    #TARGET & RENAME (The "Schema Enforcement" step)
    silver_df = bronze_df.select(
        F.col("company").alias("Company_Name"),
        F.col("title").alias("Role"),
        F.col("location").alias("Location"),
        F.col("job_url").alias("Job_URL"),
        F.col("date_posted").alias("Date_Posted"),
        F.col("description") # Kept for Gemini analysis later
    )

    #THE "TITLE-ONLY" BLACKLIST
    # Only filter by Role
    blacklist = ["Sales", "Marketing", "Compliance", "Legal", "Accounting", "HR", "Business Development"]
    black_pattern = "(?i)" + "|".join(blacklist)
    
    # Use the tilde (~) to say "Keep rows that DO NOT match this pattern"
    silver_df = silver_df.filter(~F.col("Role").rlike(black_pattern))

    #REGEX TITLE CLEANING (Shortening the long titles)
    silver_df = silver_df.withColumn("Role", F.regexp_replace(F.col("Role"), r"\(.*?\)", ""))
    # Cleans up bilingual slashes and extra spaces
    silver_df = silver_df.withColumn("Role", F.trim(F.regexp_replace(F.col("Role"), r" / .*", "")))

    #DEDUPLICATION (Final check)
    silver_df = silver_df.dropDuplicates(["Job_URL"])

    print(f"Silver Processing Complete: {silver_df.count()} high-quality tech roles remaining.")
    return silver_df

if __name__ == "__main__":
    # This allows you to see the cleaned table in your Databricks Notebook
    final_silver_df = silver_processing()
    if final_silver_df:
        display(final_silver_df)
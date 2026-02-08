from pyspark.sql import functions as F
from src.ingestion.ingest_raw import main as run_bronze
from src.utils.utils import extract_job_details # Import your new utility

def silver_processing():
    # 1. Trigger Bronze Ingestion
    bronze_df = run_bronze()
    if bronze_df is None: return None

    # 2. THE "SALES" PURGE (Keep this!)
    blacklist = ["Sales", "Marketing", "Compliance", "Legal", "Accounting", "HR"]
    black_pattern = "(?i)" + "|".join(blacklist)
    
    # We filter FIRST so we don't run Playwright on junk jobs
    pre_clean_df = bronze_df.filter(~F.col("title").rlike(black_pattern))

    # 3. ENRICHMENT LOOP (The new part)
    # Convert to Python list to use Playwright browser
    rows = pre_clean_df.collect()
    enriched_data = []

    print(f"Enriching {len(rows)} jobs with full descriptions...")

    for row in rows:
        # Use your new utility to get the REAL description and link
        print(f"Attempting to enrich: {row['title']} at {row['company']}")
        details = extract_job_details(row["job_url"])
        if details['apply_link'] == row["job_url"]:
            print(f"  --> Failed to find external link. Kept original.")
        else:
            print(f"  --> SUCCESS! Found: {details['apply_link'][:50]}...")
        
        enriched_data.append({
            "Company_Name": row["company"],
            "Role": row["title"],
            "Location": row["location"],
            "Job_URL": row["job_url"],
            "Description": details["description"], # REPLACES the 'None'
            "Apply_Link": details["apply_link"]     # The external link
        })

    
    # Convert back to Spark for Schema Enforcement
    spark = pre_clean_df.sparkSession
    silver_df = spark.createDataFrame(enriched_data)

    # Sorting Logic
    # We cast to date first so Spark doesn't sort it alphabetically
    silver_df = silver_df.withColumn("Date_Posted", F.to_date(F.col("Date_Posted"))) \
                         .orderBy(F.col("Date_Posted").desc())

    # REGEX CLEANING (Keep your cleaning logic)
    silver_df = silver_df.withColumn("Role", F.regexp_replace(F.col("Role"), r"\(.*?\)", ""))
    silver_df = silver_df.withColumn("Role", F.trim(F.regexp_replace(F.col("Role"), r" / .*", "")))

    print(f"Silver Processing Complete: {silver_df.count()} roles ready for Gemini.")
    return silver_df
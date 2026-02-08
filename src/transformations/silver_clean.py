from pyspark.sql import functions as F
from src.ingestion.ingest_raw import main as run_bronze
from src.utils.utils import extract_job_details # Import your new utility

try:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark_session = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark_session)
except ImportError:
    dbutils = None


# --- Technical Filter Function ---
def is_tech_internship(role, company, description):
    model = connect_to_gemini() # Using the connection function we built
    
    # Precise prompt for binary filtering
    prompt = f"""
    Analyze this internship posting for a Software Engineering student. 
    Criteria for 'YES': 
    - Requires technical skills in Java, Python, C, SQL, Linux, C++, C#, JavaScript, Bash, Rust, or other programming languages.
    - Involves coding, data analysis, system design, or cybersecurity.
    
    Criteria for 'NO':
    - Purely marketing, HR, sales, or business operations.
    - No programming mentioned.

    Role: {role}
    Company: {company}
    Description: {description}

    Return ONLY 'YES' or 'NO'. No other text.
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text.strip().upper() == "YES"
    except:
        return False # Safety default
    

def connect_to_gemini():
    if dbutils is not None:
        api_key = dbutils.secrets.get(scope="gemini-secrets", key="api-key")
    else:
        api_key = os.getenv("GEMINI_API_KEY") 
    
    genai.configure(api_key=api_key)
    return genai.GenerativeModel('gemini-1.5-flash')

    
def silver_processing():
    # Trigger Bronze Ingestion
    bronze_df = run_bronze()
    if bronze_df is None: return None

    gemini_model = connect_to_gemini()

    # filter 
    blacklist = ["Sales", "Marketing", "Compliance", "Legal", "Accounting", "HR"]
    black_pattern = "(?i)" + "|".join(blacklist)
    
    # We filter FIRST so we don't run Playwright on junk jobs
    pre_clean_df = bronze_df.filter(~F.col("title").rlike(black_pattern))

    # 3. ENRICHMENT LOOP (The new part)
    # Convert to Python list to use Playwright browser
    rows = pre_clean_df.collect()
    enriched_data = []

    for row in rows:
        # Use your new utility to get the REAL description and link
        print(f"Attempting to enrich: {row['title']} at {row['company']}")
        details = extract_job_details(row["job_url"])


        if is_tech_internship(gemini_model, row['title'], details["description"]):
            print(f"  --> MATCH: Keeping technical role.")
            enriched_data.append({
                "Company_Name": row["company"],
                "Role": row["title"],
                "Location": row["location"],
                "Job_URL": row["job_url"],
                "Description": details["description"],
                "Apply_Link": details["apply_link"],
                "Date_Posted": row["date_posted"] # RE-ADDED: needed for sorting
            })
        else:
            print(f"  --> SKIP: Non-technical or irrelevant role.")

    if not enriched_data:
        print("No jobs passed the Gemini technical filter.")
        return None
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
# Databricks notebook source
import sys
import os
from src.ingestion.ingest_raw import main

# --- 1. ENVIRONMENT SETUP ---
project_root = os.path.abspath('..')
if project_root not in sys.path:
    sys.path.append(project_root)

# --- 2. EXECUTION ---
# Catch the PySpark DataFrame returned by your script
final_df = main() 

# --- 3. VALIDATION & DISPLAY ---
if final_df is not None:
    # THE PORTFOLIO FIX: Convert complex types to strings for the UI
    # We drop the '_id' column or cast it to a string to prevent the Inference Error
    display_df = final_df.drop("_id") 
    
    print(f"Notebook successfully received {display_df.count()} rows.")
    
    # Render the professional interactive table
    display(display_df)
else:
    print("Notebook received None. Ensure 'return final_df' is at the end of ingest_raw.py.")

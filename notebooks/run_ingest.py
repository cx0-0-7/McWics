# Databricks notebook source
import sys
import os


# --- 1. ENVIRONMENT SETUP ---
project_root = os.path.abspath('..')
if project_root not in sys.path:
    sys.path.append(project_root)

from src.transformations.silver_clean import silver_processing

final_silver_df = silver_processing()

if final_silver_df is not None:
    # We use display() to see the beautiful, cleaned interactive table
    display(final_silver_df)
else:
    print("Silver processing returned no data..")


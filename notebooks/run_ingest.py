# Databricks notebook source
%pip install python-jobspy pymongo[srv] google-generativeai playwright databricks-sdk --upgrade

!playwright install chromium
!playwright install-deps chromium

# COMMAND ----------

import sys
import os

project_root = os.path.abspath('..')
if project_root not in sys.path:
    sys.path.append(project_root)

print(f"Path configured: {project_root}")

# COMMAND ----------
from src.transformations.silver_clean import silver_processing

final_silver_df = silver_processing()

if final_silver_df is not None:
    # We use display() to see the beautiful, cleaned interactive table
    display(final_silver_df)


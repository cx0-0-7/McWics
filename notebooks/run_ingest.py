# Databricks notebook source
import sys
import os

# --- THE FIX: ADD PROJECT ROOT TO PATH ---
# This looks for the 'src' folder in the directory above this notebook
project_root = os.path.abspath('..')
if project_root not in sys.path:
    sys.path.append(project_root)

# MAGIC %pip install pymongo[srv] python-jobspy

from src.ingestion.ingest_raw import main

if __name__ == "__main__":
    main()
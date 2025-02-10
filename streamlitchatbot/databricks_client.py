import requests
import streamlit as st
import json
import os

class DatabricksClient:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        
    def submit_job(self, code: str, table_name: str) -> dict:
        url = f"{self.host}/api/2.1/jobs/runs/submit"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "run_name": "AI Data Cleaning Job",
            "existing_cluster_id": self.cluster_id,
            "notebook_task": {
                "notebook_path": "/Users/t833547@spark.co.nz/ML-Playground/pyspark_run",
                "base_parameters": {
                    "code": code,
                    "table_name": table_name
                }
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            st.error(f"Databricks API Error: {str(e)}")
            return None
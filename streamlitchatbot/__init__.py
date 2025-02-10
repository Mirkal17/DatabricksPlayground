# app/__init__.py

"""
Data Cleaning Chatbot Package

Exposes core components for:
- Streamlit UI (main.py)
- Databricks integration (databricks_client.py)
- AI prompting (prompts.py)
- Validation (validation.py)
"""

__version__ = "0.1.0"

# Optional: Explicit exports
__all__ = ["main", "databricks_client", "prompts", "validation"]
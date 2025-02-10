import ast
import datetime
import json
import streamlit as st
import os

class CodeValidator:
    REQUIRED_KEYWORDS = {"spark", "table", "write", "withColumn"}
    FORBIDDEN_PATTERNS = {"toPandas()", "collect()", "show()"}
    ERROR_PATTERNS = {
        "cal(": "Should be col() from pyspark.sql.functions",
        "coalence": "Misspelled coalesce function",
        "spark_table": "Invalid method. Use spark.read.table()",
        "write.saveArtsfile": "Typo in saveAsTable",
        "make(": "Should be mode() for write operations"
    }
    
    @classmethod
    def validate(cls, code: str) -> bool:
        # Syntax check
        try:
            ast.parse(code)
        except SyntaxError as e:
            st.error(f"Invalid syntax: {str(e)}")
            return False
            
        # Safety checks
        if any(pattern in code for pattern in cls.FORBIDDEN_PATTERNS):
            st.error("Code contains forbidden operations")
            return False
            
        # Completeness check
        if not all(kw in code for kw in cls.REQUIRED_KEYWORDS):
            st.error("Code missing essential components")
            return False
            
        return True
    
    @staticmethod
    def validate_syntax(code: str) -> bool:
        try:
            ast.parse(code)
            return True
        except SyntaxError as e:
            print(f"Syntax Error: {e}")
            return False

    @staticmethod
    def validate_pyspark(code: str) -> bool:
        required = ["spark.read", ".write."]
        return all(kw in code for kw in required)
    

    
    @classmethod
    def detect_common_errors(cls, code: str) -> list:
        found_errors = []
        for pattern, message in cls.ERROR_PATTERNS.items():
            if pattern in code:
                found_errors.append(message)
        return found_errors
    

class FeedbackHandler:
    def __init__(self):
        self.feedback_path = os.getenv("FEEDBACK_DB_PATH")
        
    def log_feedback(self, original_code: str, corrected_code: str):
        feedback = {
            "original": original_code,
            "corrected": corrected_code,
            "timestamp": datetime.now().isoformat()
        }
        
        os.makedirs(os.path.dirname(self.feedback_path), exist_ok=True)
        with open(self.feedback_path, "a") as f:
            f.write(json.dumps(feedback) + "\n")
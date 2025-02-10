import streamlit as st
import os
from dotenv import load_dotenv
from openai import OpenAI
from prompts import get_system_prompt, get_user_prompt
from databricks_client import DatabricksClient
from validation import CodeValidator, FeedbackHandler
from datetime import datetime
import json

# Configuration
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY")) if os.getenv("OPENAI_API_KEY") else None
db_client = DatabricksClient()
validator = CodeValidator()
feedback_handler = FeedbackHandler()

def main():
    st.title("AI Data Cleaning Assistant")
    
    # User Inputs
    table_name = st.text_input("Table name (e.g. catalog.schema.table)", placeholder="default.epl_data")
    column_name = st.text_input("Column with issues", placeholder="player_age")
    cleaning_action = st.text_input("Cleaning action", placeholder="Fill missing values with 0")
    
    if not all([table_name, column_name, cleaning_action]):
        return
        
    # Generate Code
    with st.spinner("Analyzing data..."):
        try:
            # Get enhanced prompts
            system_prompt = get_system_prompt(table_schema="...")  # Add schema fetching logic
            user_prompt = get_user_prompt(table_name, column_name, cleaning_action)
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2
            )
            generated_code = response.choices[0].message.content
            
        except Exception as e:
            st.error(f"AI Error: {str(e)}")
            return
            
    # Display & Validate Code
    st.subheader("Generated Code")
    st.code(generated_code, language="python")

    # NEW: Validate syntax and PySpark components
    if not validator.validate_syntax(generated_code):
        st.error("Generated code contains syntax errors")
        st.code(generated_code, language='python')
        return

    if not validator.validate_pyspark(generated_code):
        st.error("Code missing essential PySpark components")
        return
    
    # NEW: Detect and fix common errors
    common_errors = validator.detect_common_errors(generated_code)
    if common_errors:
        st.error("Found common code issues:")
        for error in common_errors:
            st.markdown(f"-  {error}")
        st.markdown("**Adjusting code automatically...**")
        
        # Auto-correct known issues
        corrected_code = generated_code.replace("cal(", "col(").replace("coalence", "coalesce")
        st.code(corrected_code, language='python')
        generated_code = corrected_code  # Update with fixed code
    
    # Execution
    if st.button("Run in Databricks"):
        result = db_client.submit_job(generated_code, table_name)
        
        if result and "run_id" in result:
            st.success(f"Job {result['run_id']} submitted!")
            st.session_state.last_code = generated_code
        else:
            st.error("Submission failed")
            
    # Feedback System
    with st.expander("⚠️ Report Code Issues"):
        if st.toggle("Found a problem with this code?"):
            user_feedback = st.text_area("Describe the issue:", 
                                       placeholder="The code failed with error...")
            corrected_code = st.text_area("Corrected code (optional):", 
                                        value=generated_code,
                                        height=300)
            
            if st.button("Submit Feedback"):
                # Save to feedback log
                feedback_data = {
                    "original_code": generated_code,
                    "feedback": user_feedback,
                    "corrected_code": corrected_code,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Append to feedback file
                feedback_handler.log_feedback(feedback_data)
                st.success("Thank you! This helps improve the AI.")

if __name__ == "__main__":
    main()
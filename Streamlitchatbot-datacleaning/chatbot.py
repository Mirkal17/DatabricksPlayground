import streamlit as st
import os 
from openai import OpenAI
#import requests

#client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))  # This is the default and can be omitted

#st.title("Data Cleaning Chatbot")

#User Inputs
#column_name = st.text_input("Enter name of the column with missing values")
#cleaning_action = st.text_input("How would you like to clean your data?")

#if column_name and cleaning_action:
#    prompt = f""" The dataset contains missing values in the column: {column_name}.
#    The user wants to perform the following action: {cleaning_action}.
#    Generate PySpark code to clean the missing values as requested. """
#
#    response = client.chat.completions.create(
#        messages = [{ "role": "system", "content": "Your a data scientist with an expertise in data cleaning using PySpark"},
#                 {"role": "user", "content": prompt}],
#        model = "gpt-4o"
        
#    )

#    generated_code = response['choices'][0]['message']['content']

#    st.subheader("Generated PySpark Code:")
#    st.code(generated_code, language = 'python')



print("current working directory:", os.getcwd())
print("Files found:", os.listdir("."))
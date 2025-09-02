import os  
import mysql.connector
import pandas as pd
import streamlit as st
from openai import AzureOpenAI
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Azure OpenAI setup
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
endpoint = os.getenv("ENDPOINT_URL")
deployment = os.getenv("DEPLOYMENT_NAME", "gpt-5-mini")
subscription_key = os.getenv("AZURE_OPENAI_API_KEY", AZURE_OPENAI_API_KEY)

client = AzureOpenAI(
    azure_endpoint=endpoint,
    api_key=subscription_key,
    api_version="2025-01-01-preview",
)

# MySQL connection
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

def run_query(sql_query: str):
    """Run SQL query and return results or affected rows"""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor()

        sql_lower = sql_query.strip().lower()
        cursor.execute(sql_query)

        # Handle SELECT queries
        if cursor.description:
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=cols)

        # Handle INSERT, UPDATE, DELETE
        else:
            conn.commit()
            affected = cursor.rowcount

            if sql_lower.startswith("insert"):
                # Show last inserted row
                cursor.execute("SELECT * FROM employees ORDER BY id DESC LIMIT 1")
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=cols)

            elif sql_lower.startswith("update") and "where" in sql_lower:
                # Extract table name and WHERE condition
                table = sql_lower.split("update")[1].split("set")[0].strip()
                where_clause = sql_lower.split("where")[1].strip()
                check_query = f"SELECT * FROM {table} WHERE {where_clause}"
                cursor.execute(check_query)
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=cols)

            elif sql_lower.startswith("delete") and "from" in sql_lower:
                # Just show remaining rows after delete
                table = sql_lower.split("from")[1].split("where")[0].strip()
                check_query = f"SELECT * FROM {table} LIMIT 10"
                cursor.execute(check_query)
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=cols)

            else:
                df = pd.DataFrame({
                    "Message": [f"Query executed successfully. {affected} row(s) affected."]
                })

        cursor.close()
        conn.close()
        return df

    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})


def generate_sql(nl_query):
    """Send natural language query to Azure OpenAI to generate SQL"""
    chat_prompt = [
        {
            "role": "system",
            "content": (
                "You are a SQL expert. Convert the user's natural language request "
                "into a valid MySQL query. Do not explain, only return the SQL code."
            )
        },
        {
            "role": "user",
            "content": nl_query
        }
    ]
    completion = client.chat.completions.create(
        model=deployment,
        messages=chat_prompt,
        max_completion_tokens=200,
        stream=False
    )
    return completion.choices[0].message.content.strip()

# ------------------ Streamlit UI ------------------ #
st.set_page_config(page_title="Natural Language → SQL", page_icon="🗄️", layout="wide")

st.title("🗄️ Natural Language to SQL Explorer")
st.write("Type your question in plain English and let AI generate and run SQL queries on your MySQL database.")

# Input box
user_input = st.text_area("🔹 Enter your question (e.g., 'Show all employees with salary > 50000')")

if st.button("Run Query"):
    if user_input.strip():
        with st.spinner("🔎 Generating SQL query..."):
            generated_query = generate_sql(user_input)

        # Show the SQL query generated
        st.subheader("📜 Generated SQL Query")
        st.code(generated_query, language="sql")

        # Run the query
        df = run_query(generated_query)

        # Show results or confirmation
        if df is not None:
            if "Error" in df.columns:
                st.error(f"❌ Error executing query: {df['Error'][0]}")
            elif "Message" in df.columns:
                st.success(df["Message"][0])
            else:
                st.success("✅ Query executed successfully!")
                st.dataframe(df, use_container_width=True)
        else:
            st.warning("⚠️ No results found.")
    else:
        st.warning("Please enter a query.")

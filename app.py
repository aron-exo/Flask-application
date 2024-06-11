import streamlit as st
import pandas as pd
import psycopg2
import json
import os

# Database connection
def get_connection():
    return psycopg2.connect(
        host=os.getenv('SUPABASE_DB_HOST'),
        database=os.getenv('SUPABASE_DB_NAME'),
        user=os.getenv('SUPABASE_DB_USER'),
        password=os.getenv('SUPABASE_DB_PASSWORD'),
        port=os.getenv('SUPABASE_DB_PORT')
    )

# Query data from the database
def query_data(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title('Streamlit Application')

st.sidebar.header('Query Database')
query = st.sidebar.text_area('SQL Query', 'SELECT * FROM your_table LIMIT 10')

if st.sidebar.button('Run Query'):
    try:
        df = query_data(query)
        st.write(df)
    except Exception as e:
        st.error(f"Error: {e}")

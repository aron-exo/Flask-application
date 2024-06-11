import streamlit as st
import pandas as pd
import psycopg2

# Database connection function
def get_connection():
    try:
        conn = psycopg2.connect(
            host=st.secrets["db_host"],
            database=st.secrets["db_name"],
            user=st.secrets["db_user"],
            password=st.secrets["db_password"],
            port=st.secrets["db_port"]
        )
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

# Fetch all table names
def fetch_table_names(conn):
    query = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog');
    """
    tables = pd.read_sql(query, conn)
    return tables

# Fetch all tables and their geometry columns
def fetch_tables_with_geometry(conn):
    query = """
    SELECT f_table_schema, f_table_name, f_geometry_column
    FROM geometry_columns;
    """
    tables = pd.read_sql(query, conn)
    return tables

st.title('Streamlit Map Application')

# Establish database connection
conn = get_connection()
if conn:
    # Fetch and display all table names
    tables = fetch_table_names(conn)
    st.write("Tables in the database:")
    st.write(tables)

    # Fetch and display tables with geometry columns
    geom_tables = fetch_tables_with_geometry(conn)
    st.write("Tables with geometry columns:")
    st.write(geom_tables)
else:
    st.error("Failed to connect to the database.")

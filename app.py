import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
from streamlit_folium import st_folium

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
        st.write("Connection to database established.")
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

# Fetch all tables and their geometry columns from the public schema
def fetch_tables_with_geometry(conn):
    query = """
    SELECT f_table_schema, f_table_name, f_geometry_column
    FROM geometry_columns
    WHERE f_table_schema = 'public';
    """
    tables = pd.read_sql(query, conn)
    st.write("Tables with geometry columns:")
    st.write(tables)
    return tables

# Verify data in a table
def verify_table_data(conn, table_name, geom_column):
    query = f"""
    SELECT COUNT(*), ST_SRID({geom_column}) as srid, ST_AsText({geom_column}) as geom_text
    FROM public.{table_name}
    WHERE {geom_column} IS NOT NULL
    GROUP BY {geom_column}
    LIMIT 5;
    """
    st.write(f"Verifying data in table {table_name}:")
    st.write(query)
    df = pd.read_sql(query, conn)
    st.write(f"Sample data from table {table_name}:")
    st.write(df)
    return not df.empty

# Query all geometries from a specific table
def query_all_geometries(conn, table_name, geom_column):
    try:
        query = f"""
        SELECT ST_AsGeoJSON({geom_column}) as geometry
        FROM public.{table_name}
        WHERE ST_SRID({geom_column}) = 4326;
        """
        st.write(f"Running query on table {table_name}:")
        st.write(query)
        df = pd.read_sql(query, conn)
        st.write(f"Result for table {table_name}: {len(df)} rows")
        return df
    except Exception as e:
        st.error(f"Query error in table {table_name}: {e}")
        return pd.DataFrame()

# Query all geometries from all tables
def query_all_tables():
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    tables = fetch_tables_with_geometry(conn)

    all_data = []
    for index, row in tables.iterrows():
        table_name = row['f_table_name']
        geom_column = row['f_geometry_column']
        
        # Verify the data in the table
        if not verify_table_data(conn, table_name, geom_column):
            continue
        
        df = query_all_geometries(conn, table_name, geom_column)
        if not df.empty:
            df['table_name'] = table_name
            all_data.append(df)
    
    conn.close()
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# Function to add geometries to map
def add_geometries_to_map(geojson_list, map_object):
    for geojson in geojson_list:
        geometry = json.loads(geojson)
        if geometry['type'] == 'Point':
            folium.Marker(location=[geometry['coordinates'][1], geometry['coordinates'][0]]).add_to(map_object)
        elif geometry['type'] == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in geometry['coordinates']]).add_to(map_object)
        elif geometry['type'] == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in geometry['coordinates'][0]]).add_to(map_object)

st.title('Streamlit Map Application')

# Create a Folium map
m = folium.Map(location=[51.505, -0.09], zoom_start=13)

# Display the map using Streamlit-Folium
st_data = st_folium(m, width=700, height=500)

# Query all geometries from all tables and add them to the map
if st.button('Display All Geometries'):
    try:
        df = query_all_tables()
        if not df.empty:
            geojson_list = df['geometry'].tolist()
            add_geometries_to_map(geojson_list, m)
            st_data = st_folium(m, width=700, height=500)
        else:
            st.write("No geometries found in the tables.")
    except Exception as e:
        st.error(f"Error: {e}")

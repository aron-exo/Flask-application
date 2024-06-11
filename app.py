import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
from streamlit_folium import st_folium
from folium.plugins import Draw

# Database connection function
def get_connection():
    return psycopg2.connect(
        host=st.secrets["db_host"],
        database=st.secrets["db_name"],
        user=st.secrets["db_user"],
        password=st.secrets["db_password"],
        port=st.secrets["db_port"]
    )

# Fetch all tables and their geometry columns
def fetch_tables_with_geometry(conn):
    query = """
    SELECT f_table_schema, f_table_name, f_geometry_column
    FROM geometry_columns;
    """
    tables = pd.read_sql(query, conn)
    return tables

# Query data from the database for a specific table
def query_table_data(conn, table_name, geom_column, polygon_geojson):
    query = f"""
    SELECT * FROM {table_name}
    WHERE ST_Intersects(
        ST_SetSRID(ST_GeomFromGeoJSON('{polygon_geojson}'), 4326),
        {geom_column}
    );
    """
    st.write(f"Running query on table {table_name}:")
    st.write(query)
    df = pd.read_sql(query, conn)
    st.write(f"Result for table {table_name}: {len(df)} rows")
    return df

# Query data from all tables
def query_all_tables(polygon_geojson):
    conn = get_connection()
    tables = fetch_tables_with_geometry(conn)
    all_data = []

    st.write("Tables with geometry columns:")
    st.write(tables)

    for index, row in tables.iterrows():
        table_name = f"{row['f_table_schema']}.{row['f_table_name']}"
        geom_column = row['f_geometry_column']
        df = query_table_data(conn, table_name, geom_column, polygon_geojson)
        if not df.empty:
            df['table_name'] = table_name
            all_data.append(df)
    
    conn.close()
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

st.title('Streamlit Map Application')

# Create a Folium map
m = folium.Map(location=[51.505, -0.09], zoom_start=13)

# Add drawing options to the map
draw = Draw(
    export=True,
    filename='data.geojson',
    position='topleft',
    draw_options={'polyline': False, 'rectangle': False, 'circle': False, 'marker': False, 'circlemarker': False},
    edit_options={'edit': False}
)
draw.add_to(m)

# Display the map using Streamlit-Folium
st_data = st_folium(m, width=700, height=500)

# Handle the drawn polygon
if st_data and 'last_active_drawing' in st_data and st_data['last_active_drawing']:
    polygon_geojson = json.dumps(st_data['last_active_drawing']['geometry'])
    st.write('Polygon GeoJSON:', polygon_geojson)
    
    if st.button('Query Database'):
        try:
            df = query_all_tables(polygon_geojson)
            st.write("Query result:")
            st.write(df)
        except Exception as e:
            st.error(f"Error: {e}")

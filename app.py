import streamlit as st
import pandas as pd
import psycopg2
import json
import leafmap.foliumap as leafmap

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

# Fetch all tables with geometry data
def fetch_tables_with_geometry(conn):
    query = """
    SELECT table_name
    FROM information_schema.columns
    WHERE column_name = 'SHAPE' AND table_schema = 'public';
    """
    tables = pd.read_sql(query, conn)
    return tables

# Fetch data from a specific table
def fetch_data_from_table(conn, table_name):
    query = f"""
    SELECT ST_AsGeoJSON("SHAPE"::geometry) as geometry
    FROM public.{table_name}
    WHERE "SHAPE" IS NOT NULL;
    """
    df = pd.read_sql(query, conn)
    return df

# Fetch data from a specific table within a drawn polygon
def fetch_data_within_polygon(conn, table_name, polygon_geojson):
    query = f"""
    SELECT ST_AsGeoJSON("SHAPE"::geometry) as geometry
    FROM public.{table_name}
    WHERE ST_Intersects(
        "SHAPE"::geometry,
        ST_SetSRID(ST_GeomFromGeoJSON('{polygon_geojson}'), 4326)
    );
    """
    df = pd.read_sql(query, conn)
    return df

# Function to add geometries to map
def add_geometries_to_map(geojson_list, map_object):
    for geojson in geojson_list:
        if isinstance(geojson, str):
            geometry = json.loads(geojson)
        else:
            geometry = geojson

        if geometry['type'] == 'Point':
            folium.Marker(location=[geometry['coordinates'][1], geometry['coordinates'][0]]).add_to(map_object)
        elif geometry['type'] == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in geometry['coordinates']]).add_to(map_object)
        elif geometry['type'] == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in geometry['coordinates'][0]]).add_to(map_object)

st.title('Streamlit Map Application with Supabase')

# Create a Leafmap centered on Los Angeles
m = leafmap.Map(center=[34.0522, -118.2437], zoom=10)

# Display the map using Leafmap
st_data = m.to_streamlit(height=700)

conn = get_connection()
if conn:
    tables = fetch_tables_with_geometry(conn)
    st.write("Fetched tables with SHAPE column:")
    st.write(tables)

    if st.button('Fetch All Data'):
        all_data = []
        for _, row in tables.iterrows():
            table_name = row['table_name']
            df = fetch_data_from_table(conn, table_name)
            if not df.empty:
                geojson_list = df['geometry'].tolist()
                add_geometries_to_map(geojson_list, m)
                st_data = m.to_streamlit(height=700)
            all_data.append(df)
        if all_data:
            st.write("All data from tables has been fetched and displayed.")
        else:
            st.write("No data found in the tables.")
    
    if st_data and 'last_active_drawing' in st_data and st_data['last_active_drawing']:
        polygon_geojson = json.dumps(st_data['last_active_drawing']['geometry'])
        st.write('Polygon GeoJSON:', polygon_geojson)
        
        if st.button('Query Database'):
            all_data = []
            for _, row in tables.iterrows():
                table_name = row['table_name']
                df = fetch_data_within_polygon(conn, table_name, polygon_geojson)
                if not df.empty:
                    geojson_list = df['geometry'].tolist()
                    add_geometries_to_map(geojson_list, m)
                    st_data = m.to_streamlit(height=700)
                    all_data.append(df)
            if all_data:
                st.write("Data within the polygon has been fetched and displayed.")
            else:
                st.write("No geometries found within the drawn polygon.")
    conn.close()

import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
from streamlit_folium import st_folium
from folium.plugins import Draw

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

# Fetch all tables and their geometry columns from the public schema
def fetch_tables_with_geometry(conn):
    query = """
    SELECT f_table_schema, f_table_name, f_geometry_column
    FROM geometry_columns
    WHERE f_table_schema = 'public';
    """
    tables = pd.read_sql(query, conn)
    return tables

# Verify data in a table
def verify_table_data(conn, table_name, geom_column):
    query = f"""
    SELECT COUNT(*), pg_typeof({geom_column}) as geom_type, ST_AsText({geom_column}) as geom_text
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

# Query data from the database for a specific table
def query_table_data(conn, table_name, geom_column, polygon_geojson):
    try:
        query = f"""
        SELECT ST_AsGeoJSON({geom_column}) as geometry
        FROM public.{table_name}
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
    except Exception as e:
        st.error(f"Query error in table {table_name}: {e}")
        return pd.DataFrame()

# Query data from all tables
def query_all_tables(polygon_geojson):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    tables = fetch_tables_with_geometry(conn)
    st.write("Tables with geometry columns:")
    st.write(tables)

    all_data = []
    for index, row in tables.iterrows():
        table_name = row['f_table_name']
        geom_column = row['f_geometry_column']
        
        # Verify the data in the table
        if not verify_table_data(conn, table_name, geom_column):
            continue
        
        df = query_table_data(conn, table_name, geom_column, polygon_geojson)
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
            if not df.empty:
                geojson_list = df['geometry'].tolist()
                add_geometries_to_map(geojson_list, m)
                st_data = st_folium(m, width=700, height=500)
            else:
                st.write("No geometries found within the drawn polygon.")
        except Exception as e:
            st.error(f"Error: {e}")

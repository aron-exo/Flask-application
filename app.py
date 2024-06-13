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
        st.write("Connection to database established.")
        return conn
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

# Verify data in a table
def verify_table_data(conn, table_name):
    count_query = f"""
    SELECT COUNT(*)
    FROM public.{table_name}
    WHERE "SHAPE" IS NOT NULL;
    """
    type_query = f"""
    SELECT pg_typeof("SHAPE") as geom_type
    FROM public.{table_name}
    WHERE "SHAPE" IS NOT NULL
    LIMIT 1;
    """
    st.write(f"Verifying data in table {table_name}:")
    st.write(count_query)
    count_df = pd.read_sql(count_query, conn)
    st.write(type_query)
    type_df = pd.read_sql(type_query, conn)
    st.write(f"Sample data from table {table_name}:")
    st.write(count_df)
    st.write(type_df)
    return not count_df.empty and not type_df.empty

# Query geometries within a polygon from a specific table
def query_geometries_within_polygon(conn, table_name, polygon_geojson):
    try:
        query = f"""
        SELECT ST_AsGeoJSON(ST_GeomFromText(ST_AsText("SHAPE"))) as geometry
        FROM public.{table_name}
        WHERE ST_Intersects(
            ST_GeomFromText(ST_AsText("SHAPE")),
            ST_SetSRID(ST_GeomFromGeoJSON('{polygon_geojson}'), 4326)
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

# Query geometries within a polygon from all tables
def query_all_tables_within_polygon(polygon_geojson):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()

    # Assuming all public tables have the SHAPE column
    query = """
    SELECT table_name
    FROM information_schema.columns
    WHERE column_name = 'SHAPE' AND table_schema = 'public';
    """
    tables = pd.read_sql(query, conn)
    st.write("Fetched tables with SHAPE column:")
    st.write(tables)

    all_data = []
    for index, row in tables.iterrows():
        table_name = row['table_name']
        
        # Verify the data in the table
        if not verify_table_data(conn, table_name):
            st.write(f"No valid data found in table {table_name}. Skipping.")
            continue
        
        df = query_geometries_within_polygon(conn, table_name, polygon_geojson)
        if not df.empty:
            df['table_name'] = table_name
            all_data.append(df)
    
    conn.close()
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# Function to add geometries to map
def add_geometries_to_map(geojson_list, map_object):
    for geojson in geojson_list:
        st.write(f"Adding geometry to map: {geojson}")
        if isinstance(geojson, str):
            geometry = json.loads(geojson)
        else:
            geometry = geojson  # Assuming it's already a dict
        
        st.write(f"Parsed geometry: {geometry}")

        if geometry['type'] == 'Point':
            folium.Marker(location=[geometry['coordinates'][1], geometry['coordinates'][0]]).add_to(map_object)
        elif geometry['type'] == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in geometry['coordinates']]).add_to(map_object)
        elif geometry['type'] == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in geometry['coordinates'][0]]).add_to(map_object)
        else:
            st.write(f"Unsupported geometry type: {geometry['type']}")

st.title('Streamlit Map Application')

# Create a Folium map centered on Los Angeles
m = folium.Map(location=[34.0522, -118.2437], zoom_start=10)

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
            df = query_all_tables_within_polygon(polygon_geojson)
            if not df.empty:
                geojson_list = df['geometry'].tolist()
                add_geometries_to_map(geojson_list, m)
                st_data = st_folium(m, width=700, height=500)
            else:
                st.write("No geometries found within the drawn polygon.")
        except Exception as e:
            st.error(f"Error: {e}")

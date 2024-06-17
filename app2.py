import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
from streamlit_folium import st_folium
from folium.plugins import Draw
import re
import time

# Initialize session state for geometries if not already done
if 'geojson_list' not in st.session_state:
    st.session_state.geojson_list = []
if 'metadata_list' not in st.session_state:
    st.session_state.metadata_list = []
if 'map_initialized' not in st.session_state:
    st.session_state.map_initialized = False
if 'table_columns' not in st.session_state:
    st.session_state.table_columns = {}
if 'table_to_layer' not in st.session_state:
    st.session_state.table_to_layer = {}

# Database connection function with retry logic
def get_connection():
    retries = 3
    for i in range(retries):
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
            if i < retries - 1:
                time.sleep(2)  # wait before retrying
            else:
                st.error(f"Connection error after {retries} attempts: {e}")
                return None

# Query all tables with a "SHAPE" column
def get_tables_with_shape_column():
    conn = get_connection()
    if conn is None:
        return []
    try:
        query = """
        SELECT table_name 
        FROM information_schema.columns 
        WHERE column_name = 'SHAPE' AND table_schema = 'public';
        """
        df = pd.read_sql(query, conn)
        return df['table_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching table names: {e}")
        return []
    finally:
        conn.close()

# Get all layer names from the metadata table
def get_layer_names_from_metadata():
    conn = get_connection()
    if conn is None:
        return []
    try:
        query = "SELECT layer_name FROM metadata;"
        df = pd.read_sql(query, conn)
        return df['layer_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching layer names from metadata: {e}")
        return []
    finally:
        conn.close()

# Create a dictionary to map table names to layer names
def create_table_to_layer_mapping(table_names, layer_names):
    mapping = {}
    unmatched_tables = set(table_names)

    for table_name in table_names:
        sanitized_table_name = re.sub(r'\W+', '', table_name.replace('_', ' ')).lower()
        for layer_name in layer_names:
            sanitized_layer_name = re.sub(r'\W+', '', layer_name.replace(' ', '_')).lower()
            if sanitized_table_name == sanitized_layer_name:
                mapping[table_name] = layer_name
                unmatched_tables.discard(table_name)
                break

    for table_name in unmatched_tables:
        best_match = None
        best_match_score = 0
        sanitized_table_name = re.sub(r'\W+', '', table_name.replace('_', ' ')).lower()
        for layer_name in layer_names:
            sanitized_layer_name = re.sub(r'\W+', '', layer_name.replace(' ', '_')).lower()
            match_score = sum(1 for a, b in zip(sanitized_table_name, sanitized_layer_name) if a == b)
            if match_score > best_match_score:
                best_match = layer_name
                best_match_score = match_score
        if best_match:
            mapping[table_name] = best_match

    return mapping

# Get column names for a specific table
def get_table_columns(table_name):
    conn = get_connection()
    if conn is None:
        return []
    try:
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public';
        """
        df = pd.read_sql(query, conn)
        return df['column_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching columns for table {table_name}: {e}")
        return []
    finally:
        conn.close()

# Get metadata for a specific table using the mapping dictionary
def get_metadata_for_table(table_name):
    conn = get_connection()
    if conn is None:
        return None, None
    try:
        layer_name = st.session_state.table_to_layer.get(table_name)
        if not layer_name:
            st.write(f"No metadata mapping found for table {table_name}")
            return None, None
        query = f"""
        SELECT srid, drawing_info
        FROM metadata
        WHERE layer_name = '{layer_name}';
        """
        df = pd.read_sql(query, conn)
        if df.empty:
            st.write(f"No metadata found for table {table_name}")
            return None, None
        return df['srid'].iloc[0], df['drawing_info'].iloc[0]
    except Exception as e:
        st.error(f"Error fetching metadata for table {table_name}: {e}")
        return None, None
    finally:
        conn.close()

# Query geometries within a polygon for a specific table
def query_geometries_within_polygon_for_table(table_name, polygon_geojson):
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        srid, drawing_info = get_metadata_for_table(table_name)
        if srid is None:
            st.error(f"SRID not found for table {table_name}.")
            return pd.DataFrame()
        
        query = f"""
        SELECT *, "SHAPE"::text as geometry
        FROM public.{table_name}
        WHERE ST_Intersects(
            ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON("SHAPE"::json), {srid}), 4326),
            ST_SetSRID(
                ST_GeomFromGeoJSON('{polygon_geojson}'),
                4326
            )
        );
        """
        st.write(f"Executing query for table {table_name}: {query}")  # Debug statement
        df = pd.read_sql(query, conn)

        df = df.loc[:, ~df.columns.duplicated()]
        df['drawing_info'] = drawing_info

        if df.empty:
            st.write(f"No intersections found for table {table_name}.")  # Debug statement
        else:
            st.write(f"Intersections found for table {table_name}: {df.shape[0]} rows.")  # Debug statement

        return df
    except Exception as e:
        st.error(f"Query error in table {table_name}: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Query geometries within a polygon for all relevant tables
def query_geometries_within_polygon(polygon_geojson):
    tables = get_tables_with_shape_column()
    all_data = []

    layer_names = get_layer_names_from_metadata()
    st.session_state.table_to_layer = create_table_to_layer_mapping(tables, layer_names)
    
    progress_bar = st.progress(0)
    total_tables = len(tables)
    
    for idx, table in enumerate(tables):
        df = query_geometries_within_polygon_for_table(table, polygon_geojson)
        if not df.empty:
            df['table_name'] = table
            all_data.append(df)
            st.session_state.table_columns[table] = get_table_columns(table)
        progress_bar.progress((idx + 1) / total_tables)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

# Function to add geometries to map with coordinate transformation and styling
def add_geometries_to_map(geojson_list, metadata_list, map_object):
    for geojson, metadata in zip(geojson_list, metadata_list):
        if 'srid' not in metadata:
            continue

        srid = metadata.pop('srid')
        table_name = metadata.pop('table_name')
        drawing_info = metadata.pop('drawing_info', {})
        geometry = json.loads(geojson)

        src_crs = pyproj.CRS(f"EPSG:{srid}")
        dst_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        shapely_geom = shape(geometry)
        transformed_geom = transform(transformer.transform, shapely_geom)

        metadata.pop('geometry', None)
        metadata.pop('SHAPE', None)

        table_columns = st.session_state.table_columns.get(table_name, [])
        filtered_metadata = {key: value for key, value in metadata.items() if key in table_columns and pd.notna(value) and value != ''}

        st.write(f"Adding geometry to map for table {table_name}: {transformed_geom.geom_type}")  # Debug statement

        style = {}
        if 'renderer' in drawing_info:
            renderer = drawing_info['renderer']
            if 'symbol' in renderer:
                symbol = renderer['symbol']
                if 'color' in symbol:
                    style['color'] = f"rgba({symbol['color'][0]},{symbol['color'][1]},{symbol['color'][2]},{symbol['color'][3] / 255})"
                if 'outline' in symbol and 'color' in symbol['outline']:
                    style['outline_color'] = f"rgba({symbol['outline']['color'][0]},{symbol['outline']['color'][1]},{symbol['outline']['color'][2]},{symbol['outline']['color'][3] / 255})"

        # Create a popup with metadata (other columns)
        metadata_html = f"<b>Table: {table_name}</b><br>" + "<br>".join([f"<b>{key}:</b> {value}" for key, value in filtered_metadata.items()])
        popup = folium.Popup(metadata_html, max_width=300)

        if transformed_geom.geom_type == 'Point':
            st.write(f"Adding Point: {[transformed_geom.y, transformed_geom.x]}")  # Debug statement
            folium.Marker(location=[transformed_geom.y, transformed_geom.x], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'LineString':
            st.write(f"Adding LineString: {[(coord[1], coord[0]) for coord in transformed_geom.coords]}")  # Debug statement
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in transformed_geom.coords], popup=popup, color=style.get('color')).add_to(map_object)
        elif transformed_geom.geom_type == 'Polygon':
            st.write(f"Adding Polygon: {[(coord[1], coord[0]) for coord in transformed_geom.exterior.coords]}")  # Debug statement
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in transformed_geom.exterior.coords], popup=popup, color=style.get('color'), fill_color=style.get('outline_color')).add_to(map_object)
        elif transformed_geom.geom_type == 'MultiLineString':
            st.write(f"Adding MultiLineString")  # Debug statement
            for line in transformed_geom.geoms:
                st.write(f"Adding LineString in MultiLineString: {[(coord[1], coord[0]) for coord in line.coords]}")  # Debug statement
                folium.PolyLine(locations=[(coord[1], coord[0]) for coord in line.coords], popup=popup, color=style.get('color')).add_to(map_object)
        else:
            st.write(f"Unsupported geometry type: {transformed_geom.geom_type}")

st.title('Streamlit Map Application')

# Create a Folium map centered on Los Angeles if not already done
def initialize_map():
    m = folium.Map(location=[34.0522, -118.2437], zoom_start=10)
    draw = Draw(
        export=True,
        filename='data.geojson',
        position='topleft',
        draw_options={'polyline': False, 'rectangle': False, 'circle': False, 'marker': False, 'circlemarker': False},
        edit_options={'edit': False}
    )
    draw.add_to(m)
    return m

if not st.session_state.map_initialized:
    st.session_state.map = initialize_map()
    st.session_state.map_initialized = True

# Handle the drawn polygon
st_data = st_folium(st.session_state.map, width=700, height=500, key="initial_map")

if st_data and 'last_active_drawing' in st_data and st_data['last_active_drawing']:
    polygon_geojson = json.dumps(st_data['last_active_drawing']['geometry'])
    
    if st.button('Query Database'):
        try:
            df = query_geometries_within_polygon(polygon_geojson)
            if not df.empty:
                st.session_state.geojson_list = df['geometry'].tolist()
                st.session_state.metadata_list = df.to_dict(orient='records')
                
                # Clear the existing map and reinitialize it
                m = initialize_map()
                add_geometries_to_map(st.session_state.geojson_list, st.session_state.metadata_list, m)
                st.session_state.map = m
            else:
                st.write("No geometries found within the drawn polygon.")
        except Exception as e:
            st.error(f"Error: {e}")

# Display the map using Streamlit-Folium
st_folium(st.session_state.map, width=700, height=500, key="map")


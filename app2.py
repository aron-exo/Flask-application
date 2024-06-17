import streamlit as st
import pandas as pd
import psycopg2
import json
import folium
import pyproj
from shapely.geometry import shape, Polygon
from shapely.ops import transform
from streamlit_folium import st_folium
from folium.plugins import Draw

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
        conn.close()
        return df['table_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching table names: {e}")
        return []

# Get all layer names from the metadata table
def get_layer_names_from_metadata():
    conn = get_connection()
    if conn is None:
        return []
    try:
        query = "SELECT layer_name FROM metadata;"
        df = pd.read_sql(query, conn)
        conn.close()
        return df['layer_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching layer names from metadata: {e}")
        return []

# Create a dictionary to map table names to layer names
def create_table_to_layer_mapping(table_names, layer_names):
    mapping = {}
    unmatched_tables = set(table_names)

    for table_name in table_names:
        # Remove special characters, replace spaces with underscores, and convert to lowercase for matching
        sanitized_table_name = re.sub(r'\W+', '', table_name.replace('_', ' ')).lower()
        for layer_name in layer_names:
            sanitized_layer_name = re.sub(r'\W+', '', layer_name.replace(' ', '_')).lower()
            if sanitized_table_name == sanitized_layer_name:
                mapping[table_name] = layer_name
                unmatched_tables.discard(table_name)
                break

    # For unmatched tables, attempt a more flexible match
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
        conn.close()
        return df['column_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching columns for table {table_name}: {e}")
        return []

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
        conn.close()
        if df.empty:
            st.write(f"No metadata found for table {table_name}")
            return None, None
        st.write(f"Metadata for table {table_name}: {df}")
        return df['srid'].iloc[0], df['drawing_info'].iloc[0]
    except Exception as e:
        st.error(f"Error fetching metadata for table {table_name}: {e}")
        return None, None

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
        df = pd.read_sql(query, conn)
        conn.close()

        # Ensure no duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]

        # Add drawing_info to each row
        df['drawing_info'] = drawing_info

        return df
    except Exception as e:
        st.error(f"Query error in table {table_name}: {e}")
        return pd.DataFrame()

# Query geometries within a polygon for all relevant tables
def query_geometries_within_polygon(polygon_geojson):
    tables = get_tables_with_shape_column()
    all_data = []

    # Get all layer names from the metadata table
    layer_names = get_layer_names_from_metadata()

    # Create a mapping from table names to layer names
    st.session_state.table_to_layer = create_table_to_layer_mapping(tables, layer_names)

    # Print the table to layer mapping for debugging
    st.write("Table to Layer Mapping:")
    st.write(st.session_state.table_to_layer)
    
    progress_bar = st.progress(0)
    total_tables = len(tables)
    
    for idx, table in enumerate(tables):
        st.write(f"Querying table: {table}")
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

# Function to query all geometries
def query_all_geometries():
    tables = get_tables_with_shape_column()
    all_data = []

    # Get all layer names from the metadata table
    layer_names = get_layer_names_from_metadata()

    # Create a mapping from table names to layer names
    st.session_state.table_to_layer = create_table_to_layer_mapping(tables, layer_names)

    progress_bar = st.progress(0)
    total_tables = len(tables)

    for idx, table in enumerate(tables):
        st.write(f"Querying table: {table}")
        conn = get_connection()
        if conn is None:
            continue
        try:
            srid, drawing_info = get_metadata_for_table(table)
            if srid is None:
                st.error(f"SRID not found for table {table}.")
                continue
            
            query = f"""
            SELECT *, "SHAPE"::text as geometry
            FROM public.{table}
            WHERE ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON("SHAPE"::json), {srid}), 4326) IS NOT NULL;
            """
            df = pd.read_sql(query, conn)
            conn.close()

            # Ensure no duplicate columns
            df = df.loc[:, ~df.columns.duplicated()]

            # Add drawing_info to each row
            df['drawing_info'] = drawing_info

            if not df.empty:
                df['table_name'] = table
                all_data.append(df)
                st.session_state.table_columns[table] = get_table_columns(table)
        except Exception as e:
            st.error(f"Query error in table {table}: {e}")
            continue
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
        geometry = json.loads(geojson)

        # Define the source and destination coordinate systems
        src_crs = pyproj.CRS(f"EPSG:{srid}")
        dst_crs = pyproj.CRS("EPSG:4326")
        transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)

        # Transform the geometry to the geographic coordinate system
        shapely_geom = shape(geometry)
        transformed_geom = transform(transformer.transform, shapely_geom)

        # Remove the 'geometry' and 'SHAPE' fields from metadata for the popup
        metadata.pop('geometry', None)
        metadata.pop('SHAPE', None)

        # Filter metadata to include only columns from the respective table
        table_columns = st.session_state.table_columns.get(table_name, [])
        filtered_metadata = {key: value for key, value in metadata.items() if key in table_columns and pd.notna(value) and value != ''}

        # Create a popup with metadata (other columns)
        metadata_html = f"<b>Table: {table_name}</b><br>" + "<br>".join([f"<b>{key}:</b> {value}" for key, value in filtered_metadata.items()])
        popup = folium.Popup(metadata_html, max_width=300)

        if transformed_geom.geom_type == 'Point':
            folium.Marker(location=[transformed_geom.y, transformed_geom.x], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'LineString':
            folium.PolyLine(locations=[(coord[1], coord[0]) for coord in transformed_geom.coords], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'Polygon':
            folium.Polygon(locations=[(coord[1], coord[0]) for coord in transformed_geom.exterior.coords], popup=popup).add_to(map_object)
        elif transformed_geom.geom_type == 'MultiLineString':
            for line in transformed_geom.geoms:
                folium.PolyLine(locations=[(coord[1], coord[0]) for coord in line.coords], popup=popup).add_to(map_object)
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

# Button to plot all geometries from the database
if st.button('Plot All Geometries'):
    try:
        df_all = query_all_geometries()
        if not df_all.empty:
            st.session_state.geojson_list = df_all['geometry'].tolist()
            st.session_state.metadata_list = df_all.to_dict(orient='records')

            # Clear the existing map and reinitialize it
            m = initialize_map()
            add_geometries_to_map(st.session_state.geojson_list, st.session_state.metadata_list, m)
            st.session_state.map = m
        else:
            st.write("No geometries found in the database.")
    except Exception as e:
        st.error(f"Error: {e}")

# Display the map using Streamlit-Folium
st_folium(st.session_state.map, width=700, height=500, key="map")

# Add a polygon for testing
test_polygon = Polygon([[-118.325672, 33.945354], [-118.326015, 33.961017], [-118.304214, 33.960732], [-118.304729, 33.944499], [-118.325672, 33.945354]])
folium.GeoJson(test_polygon.__geo_interface__, name="Test Polygon").add_to(st.session_state.map)

# Update the map display
st_folium(st.session_state.map, width=700, height=500, key="map2")


   

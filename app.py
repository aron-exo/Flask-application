import streamlit as st
import pandas as pd
import psycopg2
import json
import plotly.express as px

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

# Main function to run the app
def main():
    st.title('Interactive GIS Map with Plotly and Streamlit')

    conn = get_connection()
    if conn is None:
        return

    tables = fetch_tables_with_geometry(conn)
    st.write("Fetched tables with SHAPE column:")
    st.write(tables)

    selected_table = st.selectbox('Select a table to visualize:', tables['table_name'])
    if selected_table:
        st.write(f"Fetching data from {selected_table}...")

        polygon_geojson = '{"type": "Polygon", "coordinates": [[[-118.269196, 33.879537], [-118.256836, 33.914873], [-118.203278, 33.897777], [-118.219757, 33.866995], [-118.269196, 33.879537]]]}'
        df = fetch_data_within_polygon(conn, selected_table, polygon_geojson)

        if not df.empty:
            st.write(f"Displaying {len(df)} geometries from {selected_table}")
            geometries = [json.loads(geom) for geom in df['geometry']]

            # Extract coordinates for visualization
            coords = []
            for geom in geometries:
                if geom['type'] == 'Point':
                    coords.append(geom['coordinates'])
                elif geom['type'] == 'LineString':
                    coords.extend(geom['coordinates'])
                elif geom['type'] == 'Polygon':
                    coords.extend(geom['coordinates'][0])

            coords_df = pd.DataFrame(coords, columns=['lon', 'lat'])
            fig = px.scatter_mapbox(coords_df, lat='lat', lon='lon', zoom=10)
            fig.update_layout(mapbox_style="open-street-map")

            st.plotly_chart(fig)
        else:
            st.write("No geometries found within the drawn polygon.")

if __name__ == "__main__":
    main()

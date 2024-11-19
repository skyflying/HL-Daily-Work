import pyproj
from pyproj import Geod, Transformer, CRS
import pandas as pd
import geopandas as gpd
from shapely import affinity
import os
import json

def convert_to_ddmmss(value):
    degrees = int(value)
    minutes = int((value - degrees) * 60)
    seconds = round((value - degrees - minutes / 60) * 3600, 3)
    return f"{abs(degrees):02}° {abs(minutes):02}' {abs(seconds):06.3f}\""


def ensure_string_columns(df):
    #for col in df.columns:
    #   df[col] = df[col].astype(str)
    return df

def calculate_grid_convergence(easting, northing, crs="EPSG:3826"):

    crs_proj = CRS(crs)
    crs_geodetic = CRS("EPSG:4326")
    

    transformer = Transformer.from_crs(crs_proj, crs_geodetic, always_xy=True)
    lon, lat = transformer.transform(easting, northing)
    
    p = pyproj.Proj(crs_proj) 
    factors = p.get_factors(lon, lat)  
    
    grid_convergence = factors.meridian_convergence
    
    return grid_convergence
    
    

def rotate_geometry(geometry, heading, grid_convergence):
    corrected_angle = heading - grid_convergence
    print(corrected_angle)
    return affinity.rotate(geometry, -(corrected_angle -150), origin=(0, 0))

def translate_geometry(geometry, xoff, yoff):
    return affinity.translate(geometry, xoff=xoff, yoff=yoff)


def process_geojson(gdf, output_filepath, file_basename):
    original_columns = [col for col in gdf.columns if col not in ['Latitude', 'Longitude', 'geometry']]
    gdf = gdf[original_columns + ['Latitude', 'Longitude', 'geometry']]
    gdf.to_file(output_filepath, driver='GeoJSON')
    
    with open(output_filepath, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    geojson_data['name'] = file_basename
    for feature in geojson_data['features']:
        props = feature['properties']
        ordered_props = {key: props[key] for key in original_columns}
        ordered_props['Latitude'] = props['Latitude']
        ordered_props['Longitude'] = props['Longitude']
        feature['properties'] = ordered_props
    
    with open(output_filepath, 'w', encoding='utf-8') as f:
        json.dump(geojson_data, f, ensure_ascii=False)
    
    print(f"GeoJSON output file created: {output_filepath}")


def process_csv_file(input_filepath, reference_shapefile_path, records_to_process):
    df = pd.read_csv(input_filepath)
    df = ensure_string_columns(df)
    

    df['easting'] = df['easting'].astype(float)
    df['northing'] = df['northing'].astype(float)
    df['heading'] = df['heading'].astype(float) 
    
    file_directory = os.path.dirname(input_filepath)
    file_basename = os.path.basename(input_filepath)
    date_str = file_basename.split('_')[-1].split('.')[0]
    
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['easting'], df['northing']), crs="EPSG:3826")
    gdf = gdf[['fid'] + [col for col in gdf.columns if col != 'fid']]
    
    gdf4326 = gdf.to_crs(epsg=4326)
    gdf4326['Longitude'] = gdf4326.geometry.x.apply(lambda x: round(x, 7))
    gdf4326['Latitude'] = gdf4326.geometry.y.apply(lambda y: round(y, 7))
    
    gdf['Longitude'] = gdf4326['Longitude']
    gdf['Latitude'] = gdf4326['Latitude']
    
    gdf4326['lon'] = gdf4326['Longitude'].apply(convert_to_ddmmss)
    gdf4326['lat'] = gdf4326['Latitude'].apply(convert_to_ddmmss)
    
    df_processed = pd.concat([df, gdf4326[['Latitude', 'Longitude', 'lat', 'lon']]], axis=1)
    
    original_geojson_filepath = os.path.join(file_directory, f"hl-wtg-jacket-location-as-built-{date_str}.gpkg")
    process_geojson(gdf, original_geojson_filepath, file_basename)
    
    ref_gdf = gpd.read_file(reference_shapefile_path)
    ref_geometry = ref_gdf.iloc[0].geometry  
    

    records_to_process = min(records_to_process, len(gdf))
    gdf_to_process = gdf.iloc[-records_to_process:]
    
    result_gdf_list = []

    for idx, row in gdf_to_process.iterrows():
        easting = row['easting']
        northing = row['northing']
        heading = row['heading'] 
        

        grid_convergence = calculate_grid_convergence(easting, northing)
        print(f"Grid convergence for {row['fou_name']}: {grid_convergence:.4f} degrees")

        # 使用 CSV 的 heading 扣除 grid convergence 進行旋轉
        print(heading,'  ',grid_convergence)
        rotated_geometry = rotate_geometry(ref_geometry, heading,round(grid_convergence,3))
        translated_geometry = translate_geometry(rotated_geometry, xoff=easting, yoff=northing)
        
        result_gdf_list.append({**row, 'geometry': translated_geometry})

    result_gdf = gpd.GeoDataFrame(result_gdf_list, crs="EPSG:3826")
    
    output_geojson_filepath = os.path.join(file_directory, f"as-built-hl22-fou-jacket-{date_str}.gpkg")
    process_geojson(result_gdf, output_geojson_filepath, file_basename)
    

    output_excel_filepath = os.path.join(file_directory, f"hl-wtg-jacket-location-as-built-{date_str}.xlsx")
    with pd.ExcelWriter(output_excel_filepath, engine='openpyxl') as writer:
        df_processed.to_excel(writer, index=False)
        
        worksheet = writer.sheets['Sheet1']
        
        for column in df_processed:
            max_length = max(df_processed[column].astype(str).map(len).max(), len(column))
            col_idx = df_processed.columns.get_loc(column)
            col_letter = worksheet.cell(row=1, column=col_idx + 1).column_letter
            worksheet.column_dimensions[col_letter].width = max_length + 2

    print(f"Processed data saved to file: {output_excel_filepath}")

def main():
    try:
        current_directory = os.getcwd()
        
        reference_shapefile_path = r"G:\Shared drives\Project Work Clients Other\Hai Long\3_Master_Data\template\fou_jacket_simplified - template.shp"
        csv_files = [f for f in os.listdir(current_directory) if f.startswith('hl-wtg-jacket-location-as-built') and f.endswith('.csv')]
        
        if csv_files:
            for csv_file in csv_files:
                input_filepath = os.path.join(current_directory, csv_file)
                records_to_process = int(input("Enter the number of records to process starting from the end of the 'fid' list: "))
                process_csv_file(input_filepath, reference_shapefile_path, records_to_process)
        else:
            print("No CSV files found that match the pattern.")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

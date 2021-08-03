import shapely
import geopandas as gpd
import pandas as pd
from pathlib import Path

root = Path('*****')
path_to_core_places_csv = Path('*****/every_core_poi_in_nyc.csv') 
print("reading")
df = pd.read_csv(path_to_core_places_csv)
print("converting to gdf")
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
print(gdf.head())
print("writing")
gdf.to_file( root / 'every_core_poi_in_nyc.shp')
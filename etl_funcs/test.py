import data_helpers

data_helpers.ESRIshp_to_geojson(inputpath='data/noaa_opendata_test.shp', output_dir='output')
data_helpers.convert_geojson_to_vectortile(inputpath='output/noaa_opendata_test.json', output_dir="output")

#data_helpers.kml_to_geojson()


import subprocess
import json


def remove_crs(path):
    with open(path, 'r') as f:
        data = json.load(f)
    
    newdata = data
    newdata.pop('crs', None)
    with open(path, 'w') as f:
        json.dump(newdata, f)
    return


def kml_to_geojson(inputpath, output_dir):
    """Convert a kml to a geojson file, and save in output dir."""
    
    bash_command = f"k2g {inputpath} {output_dir}"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return


def shp_to_geojson(inputpath, output_dir):
    """Convert an ESRI shapefile to a geojson file, and save in output dir."""
    
    outputpath = f"{output_dir}/{inputpath.split('/')[-1].split('.')[0]}.geojson"
    bash_command = f"ogr2ogr -f GeoJSON -t_srs crs:84 {outputpath} {inputpath}"
    print(bash_command)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    remove_crs(path=outputpath)

    return


def remove_empty_properties(geojson_input):
    """Remove empty properties in a geojson."""

    new_features = []
    properties = {}
    for feature in geojson_input['features']:
        for k, v in feature['properties'].items():
            myfeature = {'type': 'Feature', 'properties': {}}
            if v != '':
                properties[k] = feature['properties'][k]

            myfeature['properties'] = properties
            new_features.append(myfeature)

    geojson_input["features"] = new_features

    return geojson_input


def transform_data_to_espg4326_coords(inputpath, output_dir):
    
    outputpath = f"{output_dir}/{inputpath.split('/')[-1].split('.')[0]}.geojson"
    bash_command = f"ogr2ogr -f GeoJSON {outputpath} -t_srs EPSG:4326 {inputpath}"
    print(bash_command)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return outputpath


def convert_geojson_to_vectortile(inputpath, output_dir):
    """Convert a geojson to a vectortile."""
   
    inputpath = transform_data_to_espg4326_coords(inputpath, output_dir)
    outputpath = f"{inputpath.split('.geojson')[0]}.mbtiles"
    bash_command = f"tippecanoe -o {outputpath} {inputpath}"
    print(bash_command)
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    
    return outputpath

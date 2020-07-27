import subproces
import json
import file_helpers

def remove_crs(path):
    with open(path, 'r') as f:
        data = json.load(f)

    data.pop('crs', None)
    with open(path, 'w') as f:
        json.dump(data, f)

    return


def kml_to_geojson(filename, output_dir):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""
    
    # to do: assert file is a kml file
    bash_command = f"k2g {filename} {output_dir}"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return


def esrishp_to_geojson(filename, output_dir):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""

     # to do: assert file is a shapefile
     bash_command = f"ogr2ogr -f GeoJSON {output_dir}/{filename}.geojson {inputfile}"
     process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
     output, error = process.communicate()

    return


def remove_empty_properties(geojson_input):
    """."""
    
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


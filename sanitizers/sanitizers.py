import os
import json
import yaml

def remove_empty_properties(geojson_input):
    
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

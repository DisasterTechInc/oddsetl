from config import constants
import os
import shutil

def cleanup_the_house():
    shutil.rmtree(f'{constants.data_dir}/')
    shutil.rmtree(f'{constants.output_dir}/')

def make_data_dirs(dirs):

    for directory in dirs:
        if not os.path.exists(f'{constants.data_dir}/{directory.lower()}'):

    return

def store_blob_in_odds(data, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    blob.upload_blob(data, overwrite=True)
    print(f'Data uploaded in Azure: {blobName}')

    return

def df_to_geojson(df, properties, lat='latitude', lon='longitude'):
    geojson = {'type':'FeatureCollection', 'features':[]}
    for _, row in df.iterrows():
        feature = {'type':'Feature',
                   'properties':{},
                   'geometry':{'type':'Point',
                               'coordinates':[]}}
        feature['geometry']['coordinates'] = [float(row[lon]),float(row[lat])]
        for prop in properties:
            feature['properties'][prop] = row[prop]
        geojson['features'].append(feature)
    return geojson

def convert_to_geojson(logger, inputfile, output_dir):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""

    bash_command = None
    if "kml" in inputfile:
        bash_command = f"k2g {inputfile} {output_dir}"
    if "shp" in inputfile:
        bash_command = f"ogr2ogr -f GeoJSON {output_dir}/{inputfile.split('/')[-1].split('.')[0]}.geojson {inputfile}"

    logger.info(f'executing file conversion bash command: {bash_command}')
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return


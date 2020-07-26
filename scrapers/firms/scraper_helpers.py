import requests
import zipfile
import io  # no joke
import os
import sys
sys.path.append('../../')
from etl_funcs import db_helpers, data_helpers

def get_active_wildfire(logger, urls, creds, upload, input_dir, output_dir):
    """Given a list of links to download, get data from urls."""

    for name, url in urls._asdict().items():
        logger.info(f"Fetching {name} FIRMS data at {url}...")
        status_code = requests.get(str(url)).status_code
        logger.info(f'Url status code {status_code}: {url}')
        if status_code == 404:
            status_code = requests.get(str(url)).status_code
        if status_code == 200:
            try:
                name = url.split("/")[-1].split(".")[0]
                r = requests.get(url)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(f'{input_dir}/{name}')
            except Exception:
                logger.info(f'Could not retrieve {url}')

            files = os.listdir(f"{input_dir}/{name}")
            files = [f for f in os.listdir(f"{input_dir}/{name}") if f.endswith('.shp')]
            for firefile in files:
                datatype = '_'.join(firefile.split("_")[0:2])
                data_helpers.shp_to_geojson(inputpath=f"{input_dir}/{name}/{firefile}", output_dir=output_dir)
                if upload:
                    firefile = f"{firefile.split('/')[-1].split('.')[0]}.geojson"
                    data_helpers.remove_crs(path=f"{output_dir}/{firefile}")
                    db_helpers.store_blob_in_odds(datafile=f"{output_dir}/{firefile}",
                                                  creds=creds,
                                                  containerName='oddsetldevtest',
                                                  blobName=f"firms_{datatype}_active_wildfire_24h.geojson")

    return

import yaml
import os
import shutil
from azure.storage.blob import BlobClient
from sys import version_info
from config import constants


def get_params(args):
    """."""
    params = {}
    params['args'] = args

    return params


def get_credentials():
    """."""
    with open(constants.creds, 'r') as f:
        creds = dict(yaml.safe_load(f.read()))

    return creds


def validate_inputs(logger, params):
    """Validate inputs. """
    errors = []

    if not version_info[0] > 2:
        errors.append('Python 3+ is required to run pipeline!')

    if type(params['args']['upload']) != bool:
        errors.append('upload argument should be True or False')

    if type(params['args']['odds_container']) != str:
        errors.append('Invalid odds_container')

    return errors


def make_dirs():

    if os.path.exists(f'{constants.output_dir}/'):
        shutil.rmtree(f'{constants.output_dir}/')
    if os.path.exists(f'{constants.data_dir}/'):
        shutil.rmtree(f'{constants.data_dir}/')

    os.mkdir(f'{constants.output_dir}/')
    os.mkdir(f'{constants.data_dir}/')

    return


def cleanup_the_house():
    shutil.rmtree(f'{constants.data_dir}/')
    shutil.rmtree(f'{constants.output_dir}/')

    return


def store_blob_in_odds(logger, params, datafile, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(datafile, 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    logger.info(f"Upload successful to odds.{containerName}: {blobName}")

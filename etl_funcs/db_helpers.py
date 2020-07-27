from azure.storage.blob import BlobClient
import yaml


def get_credentials(constants):
    """.""" 
    with open(constants.creds, 'r') as f:
        creds = dict(yaml.safe_load(f.read()))
            
    return creds
    

def store_blob_in_odds(datafile, creds, containerName, blobName, content_type):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=creds['connectionString'], container_name=containerName, blob_name=blobName)
    with open(f"{datafile}", 'rb') as f:
        my_content_settings = ContentSettings(content_type)
        blob.upload_blob(f, overwrite=True, content_settings=my_content_settings)

    return

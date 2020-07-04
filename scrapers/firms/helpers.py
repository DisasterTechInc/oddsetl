import sys
from azure.storage.blob import BlobClient
import shutil
import imaplib, os
import email
import subprocess
from config import logger_config, constants

def get_message(con, creds):
    (retcode, capabilities) = con.login(creds['email_user'],creds['email_pass'])
    con.select(readonly=1)
    (retcode, messages) = con.search(None, '(ALL)') #UNSEEN
    mail_ids = str(messages[0].decode("utf-8"))
    id_list = mail_ids.split()
    
    firmsfiles = []
    for num in id_list:
        typ, data = con.fetch(num, '(RFC822)' )
        raw_email = data[0][1]
        raw_email_string = raw_email.decode('utf-8')
        email_message = email.message_from_string(raw_email_string)
        #date = str(email_message).split("(PDT)")[0].split(";")[-1].replace(" ",'').split(',')[-1].split('-')[0]

        for part in email_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue

            fileName = part.get_filename()
            if bool(fileName):
                filePath = f'{constants.data_dir}/firms_{fileName}'
                if not os.path.isfile(filePath) :
                    fp = open(filePath, 'wb')
                    fp.write(part.get_payload(decode=True))
                    fp.close()
                #subject = str(email_message).split("Subject: ", 1)[1].split("\nTo:", 1)[0]
                convert_to_geojson(myfile=filePath)
                firmsfiles.append(f"firms_{fileName.split('.')[0]}")

    return firmsfiles 

def cleanup_the_house():
    shutil.rmtree(f'{constants.data_dir}/')
    shutil.rmtree(f'{constants.output_dir}/')

def convert_to_geojson(myfile):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""
    bashCommand = f"k2g {myfile} {constants.output_dir}"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()

    return

def store_blob_in_odds(logger, datafile, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(f"{datafile}", 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    headers = {"Authorization": "Bearer %s" %token, "content-type":"application/json"}
    logger.info(f"Upload successful to odds.{containerName}: {blobName}")

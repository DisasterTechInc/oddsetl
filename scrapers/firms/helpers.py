import sys
import requests 
import zipfile
import io  # no joke 
from azure.storage.blob import BlobClient
import shutil
import os
import email
import subprocess
from datetime import datetime
from config import constants


def get_email_message(logger, con, creds, input_dir, output_dir):
    """Get email messages from gmail Inbox"""

    try:
        (retcode, capabilities) = con.login(creds['email_user'], creds['email_pass'])
        con.select(readonly=1)
        current_date = datetime.today().strftime('%d-%b-%Y')
        since_date = datetime.strptime(current_date, "%d-%b-%Y")
        typ, [msg_ids] = con.search(None, '(since "%s")' % (since_date.strftime("%d-%b-%Y")))
        mail_ids = str([msg_ids][0].decode("utf-8"))
        id_list = mail_ids.split()

        firmsfiles = []
        for num in id_list:
            typ, data = con.fetch(num, '(RFC822)')
            raw_email_string = data[0][1].decode('utf-8')
            email_message = email.message_from_string(raw_email_string)
            # filter mail for spam - only download FIRMS alerts
            subject = str(email_message).split("Subject: ", 1)[1].split("\nTo:", 1)[0]

            if 'FIRMS Rapid Alert' not in subject:
                logger.info(f'POSSIBLE EMAIL SPAM, email subject: {subject}')
            
            else:
                for part in email_message.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue

                    fileName = part.get_filename()
                    if bool(fileName):
                        filePath = f'{input_dir}/firms_{fileName}'
                        if not os.path.isfile(filePath):
                            fp = open(filePath, 'wb')
                            fp.write(part.get_payload(decode=True))
                            fp.close()
                        logger.info(f'Converting file {filePath}')
                        convert_to_geojson(logger, inputfile=filePath, output_dir=output_dir)
                        firmsfiles.append(f"firms_{fileName.split('.')[0]}")
    
    finally:
        try:
            con.close()
        finally:
            con.logout()

    return firmsfiles 


def get_nrt_fire_alerts(logger, con, creds, upload, input_dir, output_dir):
    """."""

    try:
        firmsfiles = get_email_message(logger, con, creds, input_dir, output_dir)
    except Exception:
        logger.info(sys.exc_info()[1])
        logger.info("Could not retrieve firmsfiles from email inbox")

    if firmsfiles:
        if upload:
            firmsalert_file = None
           
            for firmsfile in firmsfiles:
                firmsalert_file = f"{firmsfile.split('/')[-1].split('.')[0]}.geojson"
                alert = f"{firmsalert_file.split('_')[-1].split('.')[0]}"
           
                logger.info(f"Importing latest {firmsalert_file} rapid alerts to odds db")
                store_blob_in_odds(logger=logger,
                                   datafile=f"{output_dir}/{firmsalert_file}",
                                   token=creds['TOKEN'],
                                   connectionString=creds['connectionString'],
                                   containerName='firms',
                                   blobName=f"firms_{alert}_latest_rapid_alerts.geojson")

    return


def get_active_wildfire(logger, urls, creds, upload, input_dir, output_dir):
    """Given a list of links to download, get data from urls."""

    for url in urls:
        status_code = requests.get(url).status_code
        if status_code == 404:
            logger.info(f'Url status code {status_code}: {url}')
            status_code = requests.get(url).status_code
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
                convert_to_geojson(logger, inputfile=f"{input_dir}/{name}/{firefile}", output_dir=output_dir)
                if upload:
                    firefile = f"{firefile.split('/')[-1].split('.')[0]}.geojson"
                    store_blob_in_odds(logger=logger,
                                       datafile=f"{output_dir}/{firefile}",
                                       token=creds['TOKEN'],
                                       connectionString=creds['connectionString'],
                                       containerName='firms',
                                       blobName=f"firms_{datatype}_active_wildfire_24h.geojson")
   
    return


def cleanup_the_house():
    """Flush data & output dirs."""
    
    shutil.rmtree(f'{constants.alerts_input}')
    shutil.rmtree(f'{constants.alerts_output}')
    shutil.rmtree(f'{constants.activefires_input}')
    shutil.rmtree(f'{constants.activefires_output}')
    return


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


def store_blob_in_odds(logger, datafile, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(f"{datafile}", 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    logger.info(f"Upload successful to odds.{containerName}: {blobName}")

    return

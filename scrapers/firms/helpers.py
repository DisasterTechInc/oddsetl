import sys
import requests, zipfile, io
from azure.storage.blob import BlobClient
import shutil
import imaplib, os
import email
import subprocess
from config import logger_config, constants

def get_message(con, creds, input_dir, output_dir):
    (retcode, capabilities) = con.login(creds['email_user'],creds['email_pass'])
    con.select(readonly=1)
    (retcode, messages) = con.search(None, '(ALL)') #UNSEEN
    mail_ids = str(messages[0].decode("utf-8"))
    id_list = mail_ids.split()
    
    firmsfiles = []
    dates = {}
    for num in id_list:
        typ, data = con.fetch(num, '(RFC822)')
        raw_email = data[0][1]
        raw_email_string = raw_email.decode('utf-8')
        email_message = email.message_from_string(raw_email_string)
        # filter mail for spam - only download FIRMS alerts
        subject = str(email_message).split("Subject: ", 1)[1].split("\nTo:", 1)[0]
        dates['date'] = str(email_message).split("(PDT)")[0].split(";")[-1].replace(" ",'').split(',')[-1].split('-')[0]  
        dates['day'] = dates['date'].split(':')[0]
        dates['email_message'] = email_message
        dates['num'] = num
        dates['subject'] = subject
    
    import pdb;pdb.set_trace()
    for message in dates.items():
        email_message = dates['email_message']
        if 'FIRMS Rapid Alert' not in subject:
            print(subject)
            print('POSSIBLE EMAIL SPAM, email subject: {subject}')
            sys.exit()
        else:
            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue

                fileName = part.get_filename()
                if bool(fileName):
                    filePath = f'{input_dir}/firms_{fileName}'
                    if not os.path.isfile(filePath) :
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()
                    print(f'Converting file {filePath}')
                    convert_to_geojson(inputfile=filePath, output_dir=output_dir)
                    firmsfiles.append(f"firms_{fileName.split('.')[0]}")

    return firmsfiles 

def get_nrt_fire_alerts(con, creds, upload, input_dir, output_dir):
    
    firmsfiles = get_message(con, creds, input_dir, output_dir)
    if upload:
        for firmsfile in firmsfiles:
            firmsfile = f"{firmsfile.split('/')[-1].split('.')[0]}.geojson"
            alert = f"{firmsfile.split('_')[-1].split('.')[0]}"
            print(f"Importing latest {firmsfile} rapid alerts to odds db")
            store_blob_in_odds(datafile = f"{output_dir}/{firmsfile}",
                token = creds['TOKEN'],
                connectionString = creds['connectionString'],
                containerName = 'firms',
                blobName = f"firms_{alert}.geojson") # todo: inject date into data
    return

def get_active_wildfire(urls, creds, upload, input_dir, output_dir):
    """Given a list of links to download, get data from urls."""

    for url in urls:
        status_code = requests.get(url).status_code
        if status_code == 404:
            print(f'Url status code {status_code}: {url}')
            status_code = requests.get(url).status_code
        if status_code == 200:
            try:
                name = url.split("/")[-1].split(".")[0]
                r = requests.get(url)
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(f'{input_dir}/{name}')
            except Exception:
                print(f'Could not retrieve {url}')
            
            files = os.listdir(f"{input_dir}/{name}")
            
            files = os.listdir(f"{input_dir}/{name}")
            
            files = os.listdir(f"{input_dir}/{name}")
            files = [f for f in os.listdir(f"{input_dir}/{name}") if f.endswith('.shp')] 
            for firefile in files:
                datatype = '_'.join(firefile.split("_")[0:2])
                print(firefile)
                convert_to_geojson(inputfile=f"{input_dir}/{name}/{firefile}", output_dir=output_dir)
                if upload:
                    firefile = f"{firefile.split('/')[-1].split('.')[0]}.geojson"
                    store_blob_in_odds(datafile = f"{output_dir}/{firefile}",
                    token = creds['TOKEN'],
                    connectionString = creds['connectionString'],
                    containerName = 'firms',
                    blobName = f"firms_{datatype}_active_wildfire_24h.geojson") # todo: inject date into data
    return

def cleanup_the_house():
    shutil.rmtree(f'{constants.alerts_input}')
    shutil.rmtree(f'{constants.alerts_output}')
    shutil.rmtree(f'{constants.activefires_input}')
    shutil.rmtree(f'{constants.activefires_output}')

def convert_to_geojson(inputfile, output_dir):
    """Convert a kml or shapefile to a geojson file, and output in corresponding datadir."""
    
    bashCommand = None 
    if "kml" in inputfile:
        bashCommand = f"k2g {inputfile} {output_dir}"
    elif "shp" in inputfile:
        bashCommand = f"ogr2ogr -f GeoJSON {output_dir}/{inputfile.split('/')[-1].split('.')[0]}.geojson {inputfile}"    
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    return

def store_blob_in_odds(datafile, token, connectionString, containerName, blobName):
    """Store json files in db."""

    blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=blobName)
    with open(f"{datafile}", 'rb') as f:
        blob.upload_blob(f, overwrite=True)

    headers = {"Authorization": "Bearer %s" %token, "content-type":"application/json"}
    print(f"Upload successful to odds.{containerName}: {blobName}")



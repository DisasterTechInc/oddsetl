from config import logger_config, constants
import logging
import shutil
import argparse
import yaml
import helpers
import logging.config
import imaplib, os
import sys
import helpers
logger = logging.getLogger(__name__)
logging.config.dictConfig(logger_config)

parser = argparse.ArgumentParser(description='pipeline')
parser.add_argument('--loggers', type=bool, default=True)
parser.add_argument('--upload', type=bool, default=True)
parser.add_argument('--odds_container', type=str, default='firms')
args = parser.parse_args()


class Pipeline():
    def __init__(self, loggers, upload, odds_container):
        self.loggers = loggers
        self.upload = upload
        self.odds_container = odds_container
        logger.propagate = self.loggers
       
    def run(self):

        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
        
        imap_url = 'imap.googlemail.com'
        con = imaplib.IMAP4_SSL(imap_url)
       
        try:
            firmsfiles = helpers.get_message(con, creds)
        
            if self.upload:
                for firmsfile in firmsfiles:
                    firmsfile = f"{firmsfile.split('/')[-1].split('.')[0]}.geojson"
                    alert = f"{firmsfile.split('_')[-1].split('.')[0]}" 
                    print(f"Importing latest {firmsfile} rapid alerts to odds db")
                    helpers.store_blob_in_odds(logger, 
                        datafile = f"{constants.output_dir}/{firmsfile}", 
                        token = creds['TOKEN'], 
                        connectionString = creds['connectionString'], 
                        containerName = self.odds_container, 
                        blobName = f"firms_{alert}.geojson") # todo: inject date into data if not already
        
        except:
            print(sys.exc_info()[1])
            sys.exit(1)
        
        helpers.cleanup_the_house()

if __name__ == "__main__":
    
    if os.path.exists(f'{constants.output_dir}/'):
        shutil.rmtree(f'{constants.output_dir}/')
    if os.path.exists(f'{constants.data_dir}/'):
        shutil.rmtree(f'{constants.data_dir}/')

    os.mkdir(f'{constants.output_dir}/')
    os.mkdir(f'{constants.data_dir}/')

    pipeline = Pipeline(
        loggers = args.loggers,
        upload = args.upload, 
        odds_container = args.odds_container
        )

    pipeline.run()


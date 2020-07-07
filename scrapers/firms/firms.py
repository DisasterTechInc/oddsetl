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
#parser.add_argument('--odds_container', type=str, default='firms')
args = parser.parse_args()


class Pipeline():
    def __init__(self, loggers, upload):
        self.loggers = loggers
        self.upload = upload
        logger.propagate = self.loggers
       
    def run(self):

        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
       
        con = imaplib.IMAP4_SSL('imap.googlemail.com')
        
        urls = ['https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/shapes/zips/MODIS_C6_Global_24h.zip',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Global_24h.zip',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_Global_24h.zip']
        try:
            helpers.get_active_wildfire(urls=urls, 
                    creds=creds,
                    upload = self.upload,
                    input_dir=constants.activefires_input, output_dir=constants.activefires_output)
        except Exception:
            print(sys.exc_info()[1])

        #try:
        #    helpers.get_nrt_fire_alerts(con=con, 
        #            creds=creds, 
        #            upload=self.upload, 
        #            input_dir=constants.alerts_input, 
        #            output_dir=constants.alerts_output)
        #except Exception:
        #    print(sys.exc_info()[1])

        helpers.cleanup_the_house()

if __name__ == "__main__":
    
    if not os.path.exists(f'{constants.activefires_input}/'):
        os.mkdir(f'{constants.activefires_input}/')
    if not os.path.exists(f'{constants.activefires_output}/'):
        os.mkdir(f'{constants.activefires_output}/')
    if not os.path.exists(f'{constants.alerts_input}/'):
        os.mkdir(f'{constants.alerts_input}/')
    if not os.path.exists(f'{constants.alerts_output}/'):
        os.mkdir(f'{constants.alerts_output}/')

    pipeline = Pipeline(
        loggers = args.loggers,
        upload = args.upload, 
        )

    pipeline.run()


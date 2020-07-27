from config import logger_config, constants
import logging
import argparse
import yaml
import helpers
import logging.config
import imaplib; imaplib.Debug=True
import os
import sys
from datetime import datetime
logger = logging.getLogger(__name__)
logging.config.dictConfig(logger_config)

parser = argparse.ArgumentParser(description='FIRMS-scraper')
parser.add_argument('--loggers', type=bool, default=True)
parser.add_argument('--upload', type=bool, default=True)
args = parser.parse_args()


class FIRMS():
    def __init__(self, loggers, upload):
        self.loggers = loggers
        self.upload = upload
        logger.propagate = logger
       
    def run(self):

        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
       
        urls = ['https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/shapes/zips/MODIS_C6_USA_contiguous_and_Hawaii_24h.zip',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_USA_contiguous_and_Hawaii_24h.zip',
                'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/shapes/zips/J1_VIIRS_C2_USA_contiguous_and_Hawaii_24h.zip'] 
        try:
            logger.info('Retrieving Active fires....')
            helpers.get_active_wildfire(logger=logger,
                                        urls=urls, 
                                        creds=creds,
                                        upload=self.upload,
                                        input_dir=constants.activefires_input, 
                                        output_dir=constants.activefires_output)
        except Exception:
            logger.info(sys.exc_info()[1])
            logger.info('Could not retrieve active wildfire data')
    
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

    pipeline = FIRMS(
        loggers=args.loggers,
        upload=args.upload)

    pipeline.run()

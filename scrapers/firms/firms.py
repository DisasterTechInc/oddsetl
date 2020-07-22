from config import logger_config, constants, data_urls
import logging
import argparse
import yaml
import helpers
import logging.config
import imaplib; imaplib.Debug=True
import os
import sys
from utils import utils
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
        creds = utils.get_credentials()
         
        try:
            logger.info('Retrieving Active fires....')
            helpers.get_active_wildfire(logger=logger,
                                        urls=data_urls, 
                                        creds=creds,
                                        upload=self.upload,
                                        input_dir=constants.activefires_input, 
                                        output_dir=constants.activefires_output)
        except Exception:
            logger.info(sys.exc_info()[0])
            logger.info(sys.exc_info()[1])
            logger.info('Could not retrieve active wildfire data')
    
        utils.cleanup_the_house()


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

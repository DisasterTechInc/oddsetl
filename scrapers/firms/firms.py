from config import logger_config, constants, data_urls
from etl_funcs import db_helpers, file_helpers
import logging
import logging.config
import scraper_helpers
import argparse
import sys
sys.path.append('../../')
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
        dirs_lst = [constants.activefires_input, constants.activefires_output, constants.alerts_input, constants.alerts_output]
        file_helpers.make_dirs(dirs_lst)
        creds = db_helpers.get_credentials(constants)

        try:
            logger.info('Retrieving Active fires....')
            scraper_helpers.get_active_wildfire(logger=logger,
                                                urls=data_urls,
                                                creds=creds,
                                                upload=self.upload,
                                                input_dir=constants.activefires_input,
                                                output_dir=constants.activefires_output)
        except Exception:
            logger.info(sys.exc_info()[0])
            logger.info(sys.exc_info()[1])
            logger.info('Could not retrieve active wildfire data')

        file_helpers.cleanup_the_house(dirs_lst)


if __name__ == "__main__":

    pipeline = FIRMS(
        loggers=args.loggers,
        upload=args.upload)

    pipeline.run()

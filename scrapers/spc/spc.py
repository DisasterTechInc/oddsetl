import os
from config import logger_config, constants
import logging
import argparse
import datetime
import helpers
import utils
import logging.config
logger = logging.getLogger(__name__)
logging.config.dictConfig(logger_config)

parser = argparse.ArgumentParser(description='SPC-Watch-scraper')
parser.add_argument('--loggers', type=bool, default=True)
parser.add_argument('--upload', type=bool, default=True)
parser.add_argument('--odds_container', type=str, default='spc')
args = parser.parse_args()


class SPC():
    def __init__(self, loggers, upload, odds_container):
        self.loggers = loggers
        self.upload = upload
        self.odds_container = odds_container
        logger.propagate = self.loggers

    def run(self):

        creds = utils.get_credentials()
        args = {'upload': self.upload, 'odds_container': self.odds_container}
        params = utils.get_params(args)
        utils.make_dirs()
        errors = utils.validate_inputs(logger, params)

        if not errors:
            watches = helpers.get_thunderstorm_watches(logger, url="https://www.spc.noaa.gov/products/watch/")
            helpers.get_watch_report(watches)

            if self.upload:
                for report in os.listdir(constants.output_dir):
                    reportname = f"{datetime.datetime.now().isoformat()}_{report}"
                    print(reportname)
                    utils.store_blob_in_odds(logger=logger,
                                             params=params,
                                             datafile=f"{constants.output_dir}/{report}",
                                             token=creds['TOKEN'],
                                             connectionString=creds['connectionString'],
                                             containerName=self.odds_container,
                                             blobName=reportname)

            utils.cleanup_the_house()


if __name__ == "__main__":

    pipeline = SPC(loggers=args.loggers,
                   upload=args.upload,
                   odds_container=args.odds_container)

    pipeline.run()

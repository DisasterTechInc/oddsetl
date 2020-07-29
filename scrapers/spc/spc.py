import os
from config import logger_config, constants
import logging
import argparse
import yaml
import shutil
import datetime
import scraper_helpers
import logging.config
from etl_funcs import db_helpers, file_helpers
import sys
sys.path.append('../../')
logger = logging.getLogger(__name__)
logging.config.dictConfig(logger_config)

parser = argparse.ArgumentParser(description='SPC-Watch-scraper')
parser.add_argument('--loggers', type=bool, default=True)
parser.add_argument('--upload', type=bool, default=True)
parser.add_argument('--odds_container', type=str, default='spcreports')
args = parser.parse_args()


class SPC():
    def __init__(self, loggers, upload, odds_container):
        self.loggers = loggers
        self.upload = upload
        self.odds_container = odds_container
        logger.propagate = self.loggers
        
    def run(self):

        creds = db_helpers.get_credentials(constants)
        with open(constants.params, 'r') as f:
            params = dict(yaml.safe_load(f.read()))
        args = {'upload': self.upload, 'odds_container': self.odds_container, 'relevant_counties': params['relevant_counties']}
        file_helpers.make_dirs(dir_lst=['output'])
        watches = scraper_helpers.get_thunderstorm_watches(logger, url="https://www.spc.noaa.gov/products/watch/")
        watches = scraper_helpers.get_watch_report(watches)
       
        if self.upload:
            for report in os.listdir(constants.output_dir):
                reportname = f"{datetime.datetime.now().isoformat()}_{report}"
                db_helpers.store_blob_in_odds(datafile=f"{constants.output_dir}/{report}",
                                              creds=creds,
                                              containerName=self.odds_container,
                                              blobName=report,
                                              content_type='text/html')
           
            scraper_helpers.wrap_in_html(watches)
            db_helpers.store_blob_in_odds(datafile=f"watches.html",
                                          creds=creds,
                                          containerName=self.odds_container,
                                          blobName="watch_list.html",
                                          content_type='text/html')

            if os.path.exists("watch_counties.geojson"):
                db_helpers.store_blob_in_odds(datafile=f"watch_counties.geojson",
                                          creds=creds,
                                          containerName=self.odds_container,
                                          blobName="watch_counties.geojson",
                                          content_type='geojson')
                os.remove("watch_counties.geojson")

        file_helpers.cleanup_the_house(dir_lst=['output'])

if __name__ == "__main__":

    pipeline = SPC(loggers=args.loggers,
                   upload=args.upload,
                   odds_container=args.odds_container)

    pipeline.run()

from config import logger_config, constants
import logging
import argparse
import yaml
import helpers
import os
import logging.config
logger = logging.getLogger(__name__)
logging.config.dictConfig(logger_config)

parser = argparse.ArgumentParser(description='pipeline')
parser.add_argument('--year', type=str, default='2020')
parser.add_argument('--storms_to_get', type=str, default='')
parser.add_argument('--loggers', type=bool, default=True)
parser.add_argument('--upload', type=bool, default=True)
parser.add_argument('--scrapetype', type=str, default='')
parser.add_argument('--odds_container', type=str, default='testcontainer')
args = parser.parse_args()


class Pipeline():
    def __init__(self, loggers, year, upload, storms_to_get, scrapetype, odds_container):
        self.loggers = loggers
        self.year = year
        self.upload = upload
        self.storms_to_get = storms_to_get
        self.scrapetype = scrapetype
        self.odds_container = odds_container
        logger.propagate = self.loggers

        logger.info(f'\n Launching a new run... \n Collecting NHC data for year: {self.year}...')
        if self.storms_to_get != '':
            logger.info(f' Collecting data for a specific tropical storm: {self.storms_to_get} in year {self.year}')
 
        logger.info(f" Collecting {self.scrapetype} data... ")
 
    def run(self):
        
        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
        
        with open(constants.params, 'r') as f:
            params = dict(yaml.safe_load(f.read()))
            params['main_args'] = {'upload': self.upload, 'year': self.year, 'storms_to_get': self.storms_to_get, 'scrapetype': self.scrapetype, 'odds_container': self.odds_container}

        errors = helpers.validate_inputs(params)
        logger.info(f'Errors: {errors}')

        if not errors: 
            if self.storms_to_get != '':
                tropical_storms = [self.storms_to_get]
            else:
                tropical_storms = helpers.get_storms(logger, url=f"https://www.nhc.noaa.gov/gis/archive_forecast.php?year={self.year}")    

            active_tropical_storms = helpers.get_active_storms(logger, url="https://www.nhc.noaa.gov/cyclones/")
            
            if all(value is False for value in active_tropical_storms.values()):
                logger.info(" There are currently no active tropical storms in the Atlantic, Central North Pacific or Eastern North Pacific at this time.")
                active_tropical_storms = ["No Active Tropical Storms"]
             
            if self.scrapetype == 'active':
                tropical_storms = active_tropical_storms
                helpers.insert_storms_in_mrt(logger, creds, active_tropical_storms)
           
            logger.info(f'Tropical storms are: {tropical_storms}')
            helpers.make_dirs(tropical_storms)

            if not tropical_storms == ["No Active Tropical Storms"]:
                for tropical_storm in tropical_storms:
                    code = params[str(self.year)][tropical_storm.upper()]['code']
                    
                    forecasts, tracks = helpers.get_links(logger, url=f"https://www.nhc.noaa.gov/gis/archive_forecast_results.php?id={code}&year={self.year}&name=Tropical%{self.year[0:2]}Storm%{self.year[2:4]}{tropical_storm}")
                    
                    helpers.get_data_from_url(logger,
                            self.upload, 
                            to_download=forecasts, 
                            directory=f'{constants.data_dir}/{tropical_storm.lower()}/forecasts')
                    
                    helpers.get_data_from_url(logger,
                            self.upload, 
                            to_download=tracks, 
                            directory=f'{constants.data_dir}/{tropical_storm.lower()}/tracks')
                    
                    forecasts = helpers.convert_to_geojson(logger,
                            params, 
                            self.scrapetype, 
                            directory=f'{constants.data_dir}/{tropical_storm.lower()}/forecasts', 
                            storm=tropical_storm.lower())
                    
                    tracks = helpers.convert_to_geojson(logger,
                            params, 
                            self.scrapetype, 
                            directory=f'{constants.data_dir}/{tropical_storm.lower()}/tracks', 
                            storm=tropical_storm.lower())
                
                    if self.upload:
                        for datatype in [forecasts, tracks]:
                            for datafile in datatype:
                                jsonout = {"ownerid": "NHC",
                                "attribution": "NHC",
                                "name": datafile,
                                "version": "",
                                "address": "",
                                "server": "Azure",
                                "access": "public",
                                "rendering_defaults": "None"}
               
                                helpers.store_json_in_db( 
                                    logger,
                                    datafile = f'{constants.output_dir}/{datafile}', 
                                    jsonout= jsonout,
                                    token = creds['TOKEN'], 
                                    connectionString = creds['connectionString'], 
                                    containerName = self.odds_container, 
                                    blobName = datafile)

            helpers.cleanup_the_house()


if __name__ == "__main__":

    pipeline = Pipeline(
        loggers = args.loggers,
        year = args.year,
        upload = args.upload, 
        storms_to_get = args.storms_to_get,
        scrapetype = args.scrapetype,
        odds_container = args.odds_container
        )

    pipeline.run()

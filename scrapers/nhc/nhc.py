import sys
import shutil
from config import logger_config, constants
import logging
import argparse
import yaml
import datetime
import helpers
import os
import logging.config
logger = logging.getLogger(__name__)
logging.config.dictConfig(logger_config)

parser = argparse.ArgumentParser(description='NHC-scraper')
parser.add_argument('--year', type=str, default='2020')
parser.add_argument('--storms_to_get', type=str, default='active')
parser.add_argument('--loggers', type=bool, default=True)
parser.add_argument('--upload', type=bool, default=True)
parser.add_argument('--odds_container', type=str, default='oddsetldevtest')
args = parser.parse_args()


class NHC():
    def __init__(self, loggers, year, upload, storms_to_get, odds_container):
        self.loggers = loggers
        self.year = year
        self.upload = upload
        self.storms_to_get = storms_to_get
        self.odds_container = odds_container
        logger.propagate = self.loggers

    def run(self):
        
        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
        
        with open(constants.params, 'r') as f:
            params = dict(yaml.safe_load(f.read()))
            params['main_args'] = {'upload': self.upload, 'year': self.year, 'storms_to_get': self.storms_to_get, 'odds_container': self.odds_container}

        errors, storms_to_get = helpers.validate_inputs(logger, params)
        
        if not errors:
            if os.path.exists(f'{constants.output_dir}/'):
                shutil.rmtree(f'{constants.output_dir}/')
            if os.path.exists(f'{constants.data_dir}/'):
                shutil.rmtree(f'{constants.data_dir}/')

            os.mkdir(f'{constants.output_dir}/')
            os.mkdir(f'{constants.data_dir}/')

            filename = None
            if self.year == "2020":
                # get weather outlooks
                al_weather_outlook = helpers.get_weather_outlooks(url="https://www.nhc.noaa.gov/text/refresh/MIATWOAT+shtml/111733_MIATWOAT.shtml")
                ep_weather_outlook = helpers.get_weather_outlooks(url="https://www.nhc.noaa.gov/text/refresh/MIATWOEP+shtml/111720_MIATWOEP.shtml")
                cp_weather_outlook = helpers.get_weather_outlooks(url="https://www.nhc.noaa.gov/text/refresh/HFOTWOCP+shtml/111731_HFOTWOCP.shtml")
            
                weather_outlooks = {'Atlantic': al_weather_outlook, "Central North Pacific": cp_weather_outlook, "Eastern North Pacific": ep_weather_outlook}
                filename = f"{datetime.datetime.now().replace(microsecond=0).isoformat()}_weather_outlooks"
                helpers.store_in_json(weather_outlooks, filename)

                if self.upload:
                    helpers.store_blob_in_odds(logger,
                        params,
                        datafile = f"{constants.output_dir}/{filename}.json",
                        token = creds['TOKEN'],
                        connectionString = creds['connectionString'],
                        containerName = self.odds_container,
                        blobName = f"{filename}.json")

            storms, active_storms, active_storm_dict = [], [], {}
            if self.year == "2020":
                active_storm_regions, active_storms = helpers.get_active_storms(logger, url="https://www.nhc.noaa.gov/cyclones/")
                if not active_storm_regions:
                    logger.info(" There are currently no active tropical storms in the Atlantic, Central North Pacific or Eastern North Pacific at this time.")
                else:
                    logger.info(f"Active tropical storms: {active_storms}")
                
            if storms_to_get == "all":
                print(f"All storms requested for year {self.year}")
                storms = helpers.get_storms(logger, url=f"https://www.nhc.noaa.gov/gis/archive_forecast.php?year={self.year}")
            elif storms_to_get == "active":
                print(f"Active storms requested for year {self.year}")
                storms = active_storms  
            else:
                print(f"Storms requested for year {self.year}: {storms_to_get}")
                storms = storms_to_get
                
            # check if each storm is active
            for storm in storms:
                if storm in active_storms:
                    active_storm_dict[storm] = True
                else:
                    active_storm_dict[storm] = False
            
            helpers.make_dirs(storms)
            
            if storms:
                for tropical_storm in storms:
                    logger.info(f'Launching data collection for {self.year} tropical storm: {tropical_storm}')
                    storm_code = params[str(self.year)][tropical_storm.upper()]['code']
                    is_active = active_storm_dict[str(tropical_storm)]
                    logger.info(f"Is storm currently active? {is_active}")
                    logger.info("Retrieving forecast & track data...") 
                    
                    forecasts, tracks = helpers.get_links(logger, url=f"https://www.nhc.noaa.gov/gis/archive_forecast_results.php?id={storm_code}&year={self.year}&name=Tropical%{self.year[0:2]}Storm%{self.year[2:4]}{tropical_storm}")
                    
                    helpers.get_data_from_url(logger,
                        params,
                        self.upload, 
                        to_download=forecasts, 
                        directory=f'{constants.data_dir}/{tropical_storm.lower()}/forecasts')
                    
                    helpers.get_data_from_url(logger,
                        params,
                        self.upload, 
                        to_download=tracks, 
                        directory=f'{constants.data_dir}/{tropical_storm.lower()}/tracks')
                    
                    logger.info("Unpacking data & converting to geojsons...")
                    forecasts = helpers.convert_to_geojson(logger,
                        params, 
                        directory=f'{constants.data_dir}/{tropical_storm.lower()}/forecasts', 
                        storm=tropical_storm.lower(),
                        is_active=is_active)
                    
                    tracks = helpers.convert_to_geojson(logger,
                        params, 
                        directory=f'{constants.data_dir}/{tropical_storm.lower()}/tracks', 
                        storm=tropical_storm.lower(),
                        is_active=is_active)
                   
                    storm_timeline = helpers.get_storm_timeline(params, 
                            tropical_storm=tropical_storm,
                            is_active=is_active,
                            files=tracks)
                    
                    datatypes = {'forecasts': forecasts, 'tracks': tracks}
                    if self.upload:
                        for key in datatypes.keys():
                            data_lst = datatypes[key]
                            for datafile in data_lst:
                                logger.info(f"Importing {datafile} to Azure")
                                features = helpers.get_storm_features(params, 
                                        tropical_storm = tropical_storm.upper(), 
                                        storm_datafile = f"{constants.output_dir}/{datafile}",
                                        storm_timeline = storm_timeline)
                                filename = f"nhc_{features['UTCdatetime_iso']}_{tropical_storm}_{features['storm_type']}_{features['storm_region']}_{features['county']}_{features['state']}_{features['datatype']}"
                                #helpers.insert_in_db(logger, creds, features)
                                helpers.store_blob_in_odds(logger, 
                                    params,
                                    datafile = f"{constants.output_dir}/{datafile}", 
                                    token = creds['TOKEN'], 
                                    connectionString = creds['connectionString'], 
                                    containerName = self.odds_container, 
                                    blobName = f"{filename}.geojson")

            helpers.cleanup_the_house()


if __name__ == "__main__":

    pipeline = NHC(
        loggers = args.loggers,
        year = args.year,
        upload = args.upload, 
        storms_to_get = args.storms_to_get,
        odds_container = args.odds_container
        )

    pipeline.run()

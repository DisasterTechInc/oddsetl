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

parser = argparse.ArgumentParser(description='pipeline')
parser.add_argument('--year', type=str, default='2020')
parser.add_argument('--storms_to_get', type=str, default='active')
parser.add_argument('--loggers', type=bool, default=True)
parser.add_argument('--upload', type=bool, default=True)
parser.add_argument('--odds_container', type=str, default='testcontainer')
args = parser.parse_args()


class Pipeline():
    def __init__(self, loggers, year, upload, storms_to_get, odds_container):
        self.loggers = loggers
        self.year = year
        self.upload = upload
        self.storms_to_get = storms_to_get
        self.odds_container = odds_container
        logger.propagate = self.loggers

        logger.info(f'\n Launching a new run... \n Collecting NHC data for year: {self.year}...')
        if self.storms_to_get != '':
            logger.info(f' Collecting data for a specific tropical storm: {self.storms_to_get} in year {self.year}')
 
    def run(self):
        
        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
        
        with open(constants.params, 'r') as f:
            params = dict(yaml.safe_load(f.read()))
            params['main_args'] = {'upload': self.upload, 'year': self.year, 'storms_to_get': self.storms_to_get, 'odds_container': self.odds_container}

        errors = helpers.validate_inputs(params)

        if not errors:
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

            # get active storms in 2020
            active_tropical_storms = helpers.get_active_storms(logger, url="https://www.nhc.noaa.gov/cyclones/")

            if all(value is False for value in active_tropical_storms.values()):
                logger.info(" There are currently no active tropical storms in the Atlantic, Central North Pacific or Eastern North Pacific at this time.")
                tropical_storms = [] 
            else:
                logger.info(f'Active tropical storms are: {active_tropical_storms}')
                tropical_storms = active_tropical_storms
                active_ts = str(','.join(tropical_storms))

            # get storms requested for year
            if self.storms_to_get in ["active", "all"]:
                tropical_storms = helpers.get_storms(logger, url=f"https://www.nhc.noaa.gov/gis/archive_forecast.php?year={self.year}")
            else:
                tropical_storms = [self.storms_to_get.upper()]

            helpers.make_dirs(tropical_storms)

            if tropical_storms:
                for tropical_storm in tropical_storms:
                    print(f'Processing storm {tropical_storm}')
                    is_active = False
                    if tropical_storm in active_tropical_storms:
                        is_active = True
                    storm_code = params[str(self.year)][tropical_storm.upper()]['code']
                    
                    print("Retrieving forecast & track data...") 
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
                    
                    print("Unpacking data & converting to geojsons...")
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
                                print(f"Importing {datafile} to odds db")
                                features = helpers.get_storm_features(params, 
                                        tropical_storm = tropical_storm.upper(), 
                                        storm_datafile = f"{constants.output_dir}/{datafile}",
                                        storm_timeline = storm_timeline)
                                #f"nhc_{features['UTCdatetime_iso']}_{tropical_storm}_{storm_timeline['completion']}_{features['datatype']}"
                                filename = f"nhc_{features['UTCdatetime_iso']}_{tropical_storm}_{features['storm_type']}_{features['storm_region']}_{features['county']}_{features['state']}_{features['datatype']}"
                                helpers.insert_in_db(logger, creds, features)
                                helpers.store_blob_in_odds(logger, 
                                    params,
                                    datafile = f"{constants.output_dir}/{datafile}", 
                                    token = creds['TOKEN'], 
                                    connectionString = creds['connectionString'], 
                                    containerName = self.odds_container, 
                                    blobName = f"{filename}.geojson")

            #helpers.cleanup_the_house()


if __name__ == "__main__":

    pipeline = Pipeline(
        loggers = args.loggers,
        year = args.year,
        upload = args.upload, 
        storms_to_get = args.storms_to_get,
        odds_container = args.odds_container
        )

    pipeline.run()

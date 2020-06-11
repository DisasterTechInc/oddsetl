from sys import version_info
from config import constants
import argparse
import yaml
import helpers
import shutil
import os

parser = argparse.ArgumentParser(description='pipeline')
parser.add_argument('--year', type=str, default='2020')
parser.add_argument('--storms_to_get', type=str, default='')
parser.add_argument('--loggers', type=bool, default=False)
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
 
        print(f'\n Launching a new run... \n Collecting NHC data for year: {self.year}...')
        if self.storms_to_get != '':
            print(f' Collecting data for a specific tropical storm: {self.storms_to_get} in year {self.year}')
 
        print(f" Collecting {self.scrapetype} data... ")
 
    def run(self):
        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
        with open(constants.params, 'r') as f:
            params = dict(yaml.safe_load(f.read()))
 
        errors = self._validate_inputs(params)
        if not errors: 
            if self.storms_to_get != '':
                tropical_storms = [self.storms_to_get]
            else:
                tropical_storms = helpers.get_storms(myurl=f"https://www.nhc.noaa.gov/gis/archive_forecast.php?year={self.year}")    

            active_tropical_storms = helpers.get_active_storms(myurl="https://www.nhc.noaa.gov/cyclones/")
            
            if all(value is False for value in active_tropical_storms.values()):
                print(" There are currently no active tropical storms in the Atlantic, Central North Pacific or Eastern North Pacific at this time.")
                active_tropical_storms = ["No Active Tropical Storms"]
             
            # update MRT with list of all active tropical storms
            helpers.insert_storms_in_mrt(creds, active_tropical_storms)
            
            if self.scrapetype == 'latest':
                tropical_storms = active_tropical_storms
                helpers.insert_storms_in_mrt(creds, active_tropical_storms)
            
            if not tropical_storms == ["No Active Tropical Storms"]:
                for tropical_storm in tropical_storms:
                    os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}')
                    os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}/forecasts')
                    os.mkdir(f'{constants.data_dir}/{tropical_storm.lower()}/tracks')
                    code = params[str(self.year)][tropical_storm.upper()]['code']
                    forecasts, tracks = helpers.get_links(myurl=f"https://www.nhc.noaa.gov/gis/archive_forecast_results.php?id={code}&year={self.year}&name=Tropical%{self.year[0:2]}Storm%{self.year[2:4]}{tropical_storm}")
                    forecasts = helpers.get_data_from_url(self.upload, to_download=forecasts, directory=f'{constants.data_dir}/{tropical_storm.lower()}/forecasts')
                    tracks = helpers.get_data_from_url(self.upload, to_download=tracks, directory=f'{constants.data_dir}/{tropical_storm.lower()}/tracks')
                    forecasts = helpers.convert_to_geojson(params, self.scrapetype, directory=f'{constants.data_dir}/{tropical_storm.lower()}/forecasts', storm=tropical_storm.lower())
                    tracks = helpers.convert_to_geojson(params, self.scrapetype, directory=f'{constants.data_dir}/{tropical_storm.lower()}/tracks', storm=tropical_storm.lower())
                
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
                                    datafile = f'{constants.output_dir}/{datafile}', 
                                    jsonout= jsonout,
                                    token = creds['TOKEN'], 
                                    connectionString = creds['connectionString'], 
                                    containerName = self.odds_container, 
                                    blobName = datafile)

    def _validate_inputs(self, params):
        errors = []

        if not version_info[0] > 2:
            errors.append('Python 3+ is required to run pipeline!')

        if type(self.upload) != bool:
            errors.append('upload argument should be True or False')
        
        if type(self.year) != str:
            errors.append('year argument should be a string')
       
        if int(self.year) < 2008:
            errors.append('year should be 2008 or later')

        if int(self.year) > 2020:
            errors.append('year should be 2020 or earlier')

        if type(self.storms_to_get) != str:
            errors.append('storms_to_get argument should be a string')
       
        erroneous_storms = []
        if self.storms_to_get != '':
            for storm in [self.storms_to_get]:
                if storm.upper() not in params['all_nhc_storms']:
                    erroneous_storms.append(storm)
                    errors.append(f"You are requesting data for storms that don't exist in NHC database: {erroneous_storms}!")

        if type(self.scrapetype) != str:
            errors.append("scrapetype should be a string, either 'all' or 'latest' ")
                
        if self.scrapetype not in ["latest","all"]:
            errors.append("scrapetype should be either 'all' or 'latest' ")
        
        if type(self.odds_container) != str:
            errors.append('odds_container should be a string')
                
        if self.odds_container not in ["odds", "testcontainer", "nhc", "demos"]:
            errors.append('odds_container chosen is not allowed, please use "odds", "testcontainer", "nhc" or "demos" ')
        if errors:
            print(f'Error: {errors}')
            errors = True
            
        return errors


if __name__ == "__main__":

    if os.path.exists(f'{constants.output_dir}/'):
        shutil.rmtree(f'{constants.output_dir}/')
    if os.path.exists(f'{constants.data_dir}/'):
        shutil.rmtree(f'{constants.data_dir}/')

    os.mkdir(f'{constants.output_dir}/')
    os.mkdir(f'{constants.data_dir}/')

    pipeline = Pipeline(
        loggers = args.loggers,
        year = args.year,
        upload = args.upload, 
        storms_to_get = args.storms_to_get,
        scrapetype = args.scrapetype,
        odds_container = args.odds_container
        )

    pipeline.run()
    shutil.rmtree(f'{constants.data_dir}/')
    shutil.rmtree(f'{constants.output_dir}/')

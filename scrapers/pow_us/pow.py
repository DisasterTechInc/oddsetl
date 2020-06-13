from config import constants
import argparse
import yaml
import helpers
import shutil
import os

parser = argparse.ArgumentParser(description='pipeline')
parser.add_argument('--county', type=str, default='')
parser.add_argument('--loggers', type=bool, default=False)
parser.add_argument('--upload', type=bool, default=True)
parser.add_argument('--odds_container', type=str, default='testcontainer')
args = parser.parse_args()


class Pipeline():
    def __init__(self, loggers, county, upload, odds_container):
        self.loggers = loggers
        self.county = county 
        self.upload = upload
        self.odds_container = odds_container
 
        print(f'\n Launching a new run... \n Collecting Power Outage data for county: {self.county}...')
 
    def run(self):
        with open(constants.creds, 'r') as f:
            creds = dict(yaml.safe_load(f.read()))
        with open(constants.params, 'r') as f:
            params = dict(yaml.safe_load(f.read()))
        params["main_args"] = {"county": self.county, "upload": self.upload, "odds_container": self.odds_container}

        errors = helpers.validate_inputs(params)
        if not errors: 
            powerout, filename = helpers.get_data(url=f"https://poweroutage.us/api/json_1_4/getcityoutageinfo?key={creds['powkey']}",
                    county=self.county)    
          
            if powerout:
                if self.upload:
                    helpers.store_json_in_odds(params,
                        datafile = f'{constants.output_dir}/{filename}.geojson', 
                        token = creds['TOKEN'], 
                        connectionString = creds['connectionString'], 
                        containerName = self.odds_container, 
                        blobName = f'{filename}.geojson')


if __name__ == "__main__":

    if os.path.exists(f'{constants.output_dir}/'):
        shutil.rmtree(f'{constants.output_dir}/')
    if os.path.exists(f'{constants.data_dir}/'):
        shutil.rmtree(f'{constants.data_dir}/')

    os.mkdir(f'{constants.output_dir}/')
    os.mkdir(f'{constants.data_dir}/')

    pipeline = Pipeline(
        loggers = args.loggers,
        county = args.county,
        upload = args.upload, 
        odds_container = args.odds_container
        )

    pipeline.run()
    shutil.rmtree(f'{constants.data_dir}/')
    shutil.rmtree(f'{constants.output_dir}/')

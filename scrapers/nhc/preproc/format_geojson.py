import os
from geojson_rewind import rewind
import json
from config import constants
import argparse
import yaml
import shutil

parser = argparse.ArgumentParser(description='preprocessing_pipeline')
parser.add_argument('--loggers', type=bool, default=False)
parser.add_argument('--path_to_files', type=str, default='')
args = parser.parse_args()

class Pipeline():
    def __init__(self, loggers, path_to_files):
        self.loggers = loggers
        self.path_to_files = path_to_files
    def run(self):

        filelst = os.listdir(self.path_to_files)
        for myfile in filelst:
            print(f"Reformatting {myfile}...")
            self.reformat_files(myfile)

    # remove crs opject, realign, validate and rename geojson in lowercase
    def reformat_files(self, myfile):
        filename = myfile.lower()
        with open(f"{constants.data_dir}/{myfile}") as f:
            inputd = json.load(f)
            inputd = self.remove_crs(inputd) 
            inputd = self.realign(inputd) 
        with open(f"{constants.output_dir}/{filename}", 'w') as f:
            json.dump(inputd, f)
        
        return

    def realign(self, inputd):
        inputd = rewind(inputd)
        return inputd

    def remove_crs(self, inputd):
        inputd.pop("crs")
        return inputd



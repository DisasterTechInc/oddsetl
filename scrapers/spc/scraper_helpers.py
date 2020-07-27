import pandas as pd
import requests
from config import constants
from bs4 import BeautifulSoup
import sys
sys.path.append('../../')
import etl_funcs.db_helpers, etl_funcs.file_helpers


def get_thunderstorm_watches(logger, url):
    """ Scrape SPC url, parse and retrieve Tropical storms from content."""

    soup = BeautifulSoup(requests.get(url).text, features="html.parser")
    watches = {}
    i = 0
    for element in soup.find_all('a'):
        link = None
        if 'Thunderstorm Watch' in element.get_text():
            watches[i] = {}
            element = ''.join(str(element).split('<a href="')[-1].split('"')[0]).split("watch")[-1]
            link = f"https://www.spc.noaa.gov/products/watch{element}"
            watch_name = str(element).split(".")[0].strip("/")
            print(f"...Current active watch: {link}")
            watches[i]['name'] = watch_name
            watches[i]['link'] = link
            i += i 
        else:
            watches[i] = {}
            watches[i]['name'] = 'There are currently no severe thunderstorm watches in this region'
            watches[i]['link'] =''
            
    return watches

def filter_counties(watch_counties_lst):
    """Get only counties that are in watch."""

    with open('us_counties.geojson', 'r') as f:
        data = json.load(f)
    
    new_features = []
    properties = {}
    for feature in data['features']:
        print(feature['properties'])
        if feature['properties']['STATE'] in watch_counties_lst:
            new_features.append(feature)

    # to do: adjust fill color depending on warning type
    data["features"] = new_features
    with open('watch_counties.geojson', 'w') as f:
        json.dump(data, f)

    return


def wrap_in_html(watches):

    with open('watches.html') as html_file:
        soup = BeautifulSoup(html_file.read(), features='html.parser')

    summaries = []
    for watch in watches:
       summaries.append(watches[watch]['summary'])

    for tag in soup.find_all(id="0"):
        tag.string.replace_with('\n\n'.join(summaries))

    new_text = soup.prettify()
    new_text = new_text.encode("ascii", "ignore")
    new_text = new_text.decode()

    with open('watches.html', mode='w') as new_html_file:
        new_html_file.write(new_text)
    
    return


def get_watch_report(watches):
    """Given a list of urls, retrieve current watch reports, if any exist."""
  
    for watch in watches.keys():
        url = watches[watch]['link']
        watches[watch]['summary'] = 'There are currently no severe thunderstorm watches in this region' 
        if 'http' in url:
            soup = BeautifulSoup(requests.get(url).text, features="html.parser")
            report_contents = []

            for element in soup.find_all('pre'):
                element = element.get_text()
                element = element.split("URGENT - IMMEDIATE BROADCAST REQUESTED")[-1].split("OTHER WATCH INFORMATION...")[0]
                element = element.replace("&&", "")
                report_contents.append(element)
                summary = element.split("SUMMARY")[-1].split("PRECAUTIONARY/PREPAREDNESS ACTIONS...")[0] 
                watches[watch]['summary'] = summary

            text_file = open(f"{constants.output_dir}/watch_report_{watch}.txt", "w")
            text_file.write(str(' \n\n\n'.join(report_contents)))
            text_file.close()
    
    return watches 

import pandas as pd
import requests
import json
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

def get_counties_affected(watch):

    watch['name'] = 'ww0403'
    counties_affected_link = f"https://www.spc.noaa.gov/products/watch/wou{watch['name'].replace('ww','')}.html"
    #counties_affected_link = 'https://www.spc.noaa.gov/products/watch/2019/wou0212.html' 
    soup = BeautifulSoup(requests.get(counties_affected_link).text, features="html.parser")
    for element in soup.find_all('pre'):
        element = element.get_text()
        if "KANSAS COUNTIES" in element:
            watch_counties_lst = element.split("KANSAS COUNTIES INCLUDED ARE")[-1].split("NEC")[0].split(" ")
            watch_counties_lst = ' '.join(watch_counties_lst).split()
        watch_type = element.split("IMMEDIATE BROADCAST REQUESTED")[-1].split("WATCH OUTLINE")[0]

    prepare_geojson(watch_counties_lst, watch_type)
    
    return

def prepare_geojson(watch_counties_lst, watch_type):
    """Get only counties that are in watch."""

    with open('us_counties_ks_mo_ne_ia.geojson', 'r') as f:
        data = json.load(f)
   
    new_features = []
    if 'TORNADO' in watch_type.upper():
        color = '#8b0000'
    elif 'THUNDERSTORM' in watch_type.upper():
        color = '#000080'

    for feature in data['features']:
        if feature['properties']['NAME'].upper() in watch_counties_lst:
            # two kinds of colors: blue to indicate severe thunderstorm, and red to indicate tornadoes
            feature['properties']['fill_color'] = color
            new_features.append(feature)

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
    
    if summaries:
        for tag in soup.find_all(id="0"):
            tag.string.replace_with('\n\n'.join(summaries))
            #tag.string.replace("\n", "<br>")

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
        watches[watch]['summary'] = 'No current convective watches in effect.' 
        request = None
        #url = 'https://www.spc.noaa.gov/products/watch/2019/ww0212.html'
        if 'http' in url:
            request = requests.get(url)
            if request.status_code == 200:
                soup = BeautifulSoup(requests.get(url).text, features="html.parser")
                report_contents = []
                for element in soup.find_all('pre'):
                    if "IMMEDIATE BROADCAST REQUESTED" in str(element):
                        element = element.get_text() #.upper()
                        element = element.split("URGENT - IMMEDIATE BROADCAST REQUESTED")[-1].split("OTHER WATCH INFORMATION...")[0]
                        element = element.replace("&&", "")
                        report_contents.append(element)
                        watches[watch]['report'] = element
                        watches[watch]['counties_affected'] = get_counties_affected(watches[watch])
                    if "SUMMARY" in element:
                        summary = element.split("SUMMARY")[-1].split("FOR THE FOLLOWING LOCATIONS")[0]
                        summary = summary.replace("...", "")
                        summary = summary.replace("PRECAUTIONARY/PREPAREDNESS ACTIONS", "\n\n Precautionary/Preparedness Actions recommended: \n")
                        summary = summary.replace("REMEMBER", "\n\n Remember: \n")
                        watches[watch]['summary'] = summary

                    text_file = open(f"{constants.output_dir}/watch_report_{watch}.txt", "w")
                    text_file.write(str(' \n\n\n'.join(report_contents)))
                    text_file.close()
    
    return watches 

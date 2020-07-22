import requests
from config import constants
from bs4 import BeautifulSoup


def get_thunderstorm_watches(logger, url):
    """ Scrape SPC url, parse and retrieve Tropical storms from content."""

    soup = BeautifulSoup(requests.get(url).text, features="html.parser")
    watches = {}
    for element in soup.find_all('a'):
        link = None
        if 'Thunderstorm Watch' in element.get_text():
            element = ''.join(str(element).split('<a href="')[-1].split('"')[0]).split("watch")[-1]
            link = f"https://www.spc.noaa.gov/products/watch{element}"
            watch_name = str(element).split(".")[0].strip("/")
            print(f"...Current active watch: {link}")
            watches[watch_name] = link

    return watches


def get_watch_report(watches):
    """Given a list of urls, retrieve current watch reports, if any exist."""

    for watch in watches.keys():
        url = watches[watch]
        soup = BeautifulSoup(requests.get(url).text, features="html.parser")
        report_contents = []

        #for element in soup.find_all("tbody"):
        #    element = element.get_text()
        #    print(dfs)
        #    table_contents.append(element)

        for element in soup.find_all('pre'):
            element = element.get_text()
            element = element.split("URGENT - IMMEDIATE BROADCAST REQUESTED")[-1].split("OTHER WATCH INFORMATION...")[0]
            element = element.replace("&&", "")
            report_contents.append(element)

        text_file = open(f"{constants.output_dir}/watch_report_{watch}.txt", "w")
        text_file.write(str(' \n\n\n'.join(report_contents)))
        text_file.close()

    return

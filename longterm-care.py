#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from functools import reduce
import spacy
import gensim
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from IPython.display import Image, display
import re

import requests
from bs4 import BeautifulSoup
import time
import logging
import logging.config
import yaml
import random
import os
from urllib.request import Request, urlopen
from urllib.parse import quote_plus, urlparse, parse_qs


USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15'
na_str = 'N/A'

url = 'https://www.seniorsadvocatebc.ca/quickfacts/location'
mapp = {
    'Fraser Health': 1,
    'Interior Health': 6,
    'Northern Health': 11,
    'Vancouver Coastal Health': 16,
    'Vancouver Island Health': 21
}

def getPageText(name, isMain = False):
    if pd.isna(name):
        return set()
    headers = {'User-Agent': USER_AGENT}   
    time.sleep(random.randint(1, 3))
    if random.randint(1, 11) % 10 == 0:
        time.sleep(random.randint(2, 4))
    ret = na_str
    try:
        if isMain:
            params = {"qf_s": None,
                      'qf_hlth_auth': mapp[name],
                      'qf_city': None,
                      'qf_facility': None,
                      "textDecorations": True, 
                      "textFormat": "HTML"} 
            page = requests.get(url, headers = headers, params = params).text
        else:
            page = requests.get(name, headers = headers).text
#         with open('results.html', 'w') as f:
#             f.write(page)
#         f.close()
        ret = page
    except:
        logger.warning(f'{name} has no match from {url}')
    return ret

def getFacilities(webpage):
    return [(div.a.text, div.a['href']) for div in \
                webpage.findAll('div', {'class': 'col-md-12 qf-search-record'})]

def parseAuthority(authority):
    main_text = getPageText(authority, isMain = True)
    main_page = BeautifulSoup(main_text)
    locs = getFacilities(main_page)
    anchors = main_page.findAll('ul', {'class': 'pagination'})[0].findAll(
        lambda tag: tag.name == 'li' and tag.get('class') is None and re.findall('\d', tag.a.text)
    )
    links = [anchor.a['href'] for anchor in anchors]
    for link in links:
        sub_page = BeautifulSoup(getPageText(link))
        locs += getFacilities(sub_page)
    return locs

def parseTable(tbl, offset = 0):
    tds = tbl.findAll(lambda tag: tag.name == 'td')
    return [field.text for field in tds[offset::2]], \
            [data.text for data in tds[offset + 1::2]]

def getLocation(url):
    loc1 = BeautifulSoup(getPageText(url))
    loc1_fac = loc1.findAll('table', {'class': 'table table-bordered'})[0]
    loc1_bed = loc1.findAll('table', {'class': 'table table-bordered qf-table-beds'})[0]
    loc1_room = loc1.findAll('table', {'class': 'table table-bordered qf-table-rooms'})[0]
    th = loc1_fac.findAll('th')
    head_idx, head_val = [th[0].text], [th[1].text]
    fac_idx, fac_val = parseTable(loc1_fac, offset = 1)
    bed_idx, bed_val = parseTable(loc1_bed)
    room_idx, room_val = parseTable(loc1_room)
    link_str = 'Link to web page'
    loc1_link = loc1.findAll(lambda x: x.name == 'table' \
                                 and x.th is not None \
                                 and x.th.text == link_str)[0]
    link_idx, link_val = [link_str], [loc1_link.td.a['href']]
    vals = head_val + fac_val + bed_val + room_val + link_val
    idxes = head_idx + fac_idx + bed_idx + room_idx + link_idx
    return pd.DataFrame([vals], columns = idxes)

def buildDf(auth):
    df = pd.DataFrame()
    locs = parseAuthority(authority)
    for loc in locs:
        url = loc[1]
        df = df.append(getLocation(url), ignore_index = True)
    return df

def main():
    with open('./logging.conf', 'r') as f:
        cfg = yaml.safe_load(f.read())
        logging.config.dictConfig(cfg)
    logger = logging.getLogger('all')
    logger.info('Start logging for longterm-care facility scrapping.')
    
    res = dict()
    for authority in mapp.keys():
        df = buildDf(authority)
        res[authority] = df

    with pd.ExcelWriter('longterm-care.xlsx') as writer:
        for name, tbl in res.items():
            tbl.to_excel(writer, sheet_name = name, index = False)

if __name__ == '__main__':
    main()

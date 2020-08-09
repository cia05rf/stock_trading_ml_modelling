from bs4 import BeautifulSoup as bs
import requests as rq

def get_soup(url):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = rq.get(url,headers=hdr)
    soup = bs(req.content,'html.parser')
    return soup

def refine_soup(soup, filter_li=[], log=None):
    if filter_li:
        #Descend through the filters to get to the final layer we're interested in
        for i,_filter in enumerate(filter_li):
            try:
                soup_pack = soup.find_all(**_filter)
                new_soup = []
                if len(filter_li) < i-1:
                    for pack in soup_pack:
                        new_soup = new_soup + refine_soup(pack, filter_li[i+1])
                    soup_pack = new_soup
            except:
                err_str = "each item in filter_li must be a dictionary in the format {name_tag:name,attribute_tag:attribute_value}"
                if log:
                    log.error(err_str)
                else:
                    raise Exception(err_str)
    return soup_pack
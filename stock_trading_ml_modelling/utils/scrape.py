from bs4 import BeautifulSoup as bs
import requests as rq

def get_soup(url):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    cookies = dict(BCPermissionLevel='PERSONAL')
    req = rq.get(url,headers=hdr,cookies=cookies)
    soup = bs(req.content,'html.parser')
    return soup

def refine_soup(soup, filter_li=[{}], obj_limit=[], logger=None):
    #Descend through the filters to get to the final layer we're interested in
    try:
        soup_pack = soup.find_all(**filter_li[0])
        #Reduce size of soup_pack inline with obj_limit
        if len(obj_limit) and obj_limit[0]:
            soup_pack = soup_pack[obj_limit[0]]
        else:
            obj_limit = [None] * len(filter_li)
        new_soup = []
        #Check if there are more layers
        if len(filter_li) > 1:
            for pack in soup_pack:
                new_soup = new_soup + refine_soup(pack, filter_li[1:])
            soup_pack = new_soup
    except Exception as e:
        if logger:
            logger.error(e)
        else:
            raise Exception(e)
    return soup_pack
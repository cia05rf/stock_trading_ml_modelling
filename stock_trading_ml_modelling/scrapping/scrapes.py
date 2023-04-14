"""Functions fr scrapping data from websites"""
import re
from tqdm import tqdm
import pandas as pd
import asyncio
import nest_asyncio
import aiohttp
from bs4 import BeautifulSoup as bs

from stock_trading_ml_modelling.config import WEB_ADDRS
from stock_trading_ml_modelling.utils.date import create_sec_ref_li
from stock_trading_ml_modelling.utils.data import flatten_one
from stock_trading_ml_modelling.utils.log import logger
from stock_trading_ml_modelling.utils.scrape import get_soup, refine_soup
from stock_trading_ml_modelling.utils.str_formatting import clean_col_name

def scrape_num_pages(parent:dict=WEB_ADDRS, ref:str="ftse100"):
    #Fetch the data for ftse 100
    soup = get_soup(parent.get(ref, None).format(1000))
    par_elem = refine_soup(soup, filter_li=[{"attrs":{"class":"paginator"}}])[0]
    last_page = refine_soup(par_elem, filter_li=[{"attrs":{"class":"page-number"}}])[-2] #-2 as last item is "go to last page"
    num_pages = int(re.sub('[^0-9]','', last_page.text))
    return num_pages

class AsyncScrape:
    def __init__(self, func, urls, desc=None):
        """
        args:
        ----
        func - callable - the function to run the scrape
            MUST BE ASYNC FUNCTION
        urls - list - a list of urls to be scraped
        desc - str:None - the description to be usedin the tqdm
        """
        self.func = func
        self.urls = urls
        self.desc = desc

    async def async_request(self, session, url):
        for i in range(5):
            # try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    body = await resp.content.read()
                    body = body.decode("utf-8")
                    return body
            # except Exception as e:
            #     print(f"Error encountered in attempt {i} for url {url}")
            #     print(e)
            #     continue
            # else:
            #     raise Exception(f"Failed to get a response from url {url}")

    async def get_soup(self, session, url):
        content = await self.async_request(session, url)
        soup = bs(content, 'html.parser')
        return soup
    
    async def run_func(self, session, url):
        soup = await self.get_soup(session, url)
        return self.func(soup)

    async def run_tasks(self):
        tasks = []
        semaphore = asyncio.Semaphore(1)
        hdr = {'User-Agent': 'Mozilla/5.0'}
        cookies = dict(BCPermissionLevel='PERSONAL')
        async with semaphore:
            async with aiohttp.ClientSession(headers=hdr, cookies=cookies) as session:
                for url in tqdm(self.urls, total=len(self.urls), desc=self.desc):
                    #fa will be a list of arguments
                    tasks.append(
                        self.run_func(session, url)
                    )
                return await asyncio.gather(*tasks)

    def get_resps(self):
        """Creaes an event loop and starts the async scrape"""
        loop = asyncio.get_event_loop()
        if isinstance(loop, asyncio.BaseEventLoop):
            nest_asyncio.apply()
        coro = self.run_tasks()
        resps = loop.run_until_complete(coro)
        return resps

class ScrapeTickers:
    def __init__(self, ref:str="ftse100"):
        self.ref = ref

    def process_soup(self, soup):
        row_li = []
        #Collect the table
        table_elem = refine_soup(soup, filter_li=[
            {"name":"section","class":"ftse-index-table-component"},
            {"name":"table"}
            ],
            obj_limit=[0,0])[0]
        #Collect the rows of data
        for row in refine_soup(table_elem, filter_li=[
            {"name":"tbody"},
            {"name":"tr"}
            ]):
            temp_row = []
            for cell in refine_soup(row, filter_li=[{"name":"td"}])[:2]:
                temp_row.append(re.sub('\n','', cell.text.upper()))
            row_li.append(temp_row)
        return row_li

    def scrape(self):
        num_pages = scrape_num_pages(self.ref)
        #Prep urls
        urls = [WEB_ADDRS[self.ref].format(page) for page in range(1, num_pages+1)]
        #Scrape asyncronously
        async_scrape = AsyncScrape(self.process_soup, urls)
        row_li = flatten_one(async_scrape.get_resps())
        #Create a dataframe
        tick_ftse = pd.DataFrame(data=row_li, columns=['ticker','company'])
        tick_ftse['market'] = self.ref.upper()
        return tick_ftse

class ScrapePrices:
    def __init__(self, ticker, st_date=None, en_date=None, interval="1d"):
        """
        args:
        ----
        ticker - str
        st_date - datetime object:None
        en_date - dtaetime object:None
        interval - str:"1d"

        returns:
        ----
        list
        """
        ticker = re.sub('\.', '-', ticker)
        self.ticker = f"{ticker}.L"
        self.st_date = st_date
        self.en_date = en_date
        self.interval = interval

    def process_soup(self, soup):
        if soup == "":
            logger.info('no results returned')
            #return to say that there has been an error
            return []
        #Grab the header rows
        header = refine_soup(soup, [
            {"name":"table","attrs":{'data-test':'historical-prices'}},
            {"name":"thead"},
            {"name":"tr"}
            ])[0]
        cols = [clean_col_name(th.text) for th in header]
        #Grab the data rows
        rows = refine_soup(soup, [
            {"name":"table","attrs":{'data-test':'historical-prices'}},
            {"name":"tbody"},
            {"name":"tr"}
            ])
        #If there are no dates there's no point going back further
        if len(rows) == 0:
            logger.info('No more records to collect')
            return []
        #Put the rows into the dataframe
        data = []
        for r in rows:
            td = refine_soup(r,[{"name":'td'}])
            if len(td) == len(cols):
                data.append({c:x.text for c,x in zip(cols, td)})
        return data

    def scrape(self):
        #clean ticker
        sec_ref_li = create_sec_ref_li(self.st_date, self.en_date, days=WEB_ADDRS["max_days"])
        #Prep urls
        urls = [WEB_ADDRS["share_price"].format(self.ticker, secs[0], secs[1], self.interval, self.interval) for secs in sec_ref_li]
        #Scrape asyncronously
        async_scrape = AsyncScrape(self.process_soup, urls)
        data = flatten_one(async_scrape.get_resps())
        tick_df = pd.DataFrame(data)
        return tick_df

class ScrapeBankHolidays:
    def __init__(self, year):
        self.year = year

    def process_soup(self, soup):
        if soup == "":
            logger.info('no results returned')
            #return to say that there has been an error
            return []
        #Get the data rows
        rows = refine_soup(soup, [
            {"name":"table"},
            {"name":"tbody"},
            {"name":"tr"},
            ])
        #Grab the dates
        dates = [refine_soup(r, [{"name":"td"},{"name":"span"}]) for r in rows]
        dates = [d[0].text for d in dates if len(d) > 0]
        #Grab the labels
        labels = [refine_soup(r, [{"name":"td"},{"name":"a"}]) for r in rows]
        labels = [l[0].text for l in labels if len(l) > 0]
        return list(zip(dates,labels))

    def scrape(self):
        urls = [WEB_ADDRS["holidays"].format(self.year)]
        #Scrape asyncronously
        async_scrape = AsyncScrape(self.process_soup, urls)
        data = flatten_one(async_scrape.get_resps())
        return data

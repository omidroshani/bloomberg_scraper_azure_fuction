from bs4 import BeautifulSoup
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib.request
import datetime  
from dateutil.parser import parse
import sys
import logging
import pyodbc
import re


class NewsScraper :
    def __init__(self , tickers_list) :
        self.tickers_list = tickers_list
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler(
                {
                    'http': 'Your HTTP Proxy',
                    'https': 'Your HTTPS Proxy'
                }
            )
        )
        self.retry = 10
        
        self.cnxn = pyodbc.connect("Your PyODBC Connection String")
        
        
    def insert_article_db(self , article) :
        query = f"""
        IF NOT EXISTS ( SELECT * FROM articles WHERE url = '{article['url']}' )

            INSERT INTO articles(url , ticker , title , publishedat , pulledat) 
            VALUES('{article['url']}' , '{article['ticker']}' , '{article['title'].replace("'","''")}' , '{parse(article['publishedAt']).strftime('%Y-%m-%d %H:%M:%S')}' , '{parse(article['pulledAt']).strftime('%Y-%m-%d %H:%M:%S')}')

        ELSE

            UPDATE articles 
            SET ticker = '{article['ticker']}' , title = '{article['title'].replace("'","''")}' , publishedat = '{parse(article['publishedAt']).strftime('%Y-%m-%d %H:%M:%S')}' , pulledat = '{parse(article['pulledAt']).strftime('%Y-%m-%d %H:%M:%S')}' 
            WHERE url = '{article['url']}'
        """
        for i in range(self.retry) :
            try :
                cursor = self.cnxn.cursor()
                cursor.execute(query)
                self.cnxn.commit()
                return None
            except :
                logging.info(f"Can not insert {article} into database for {i+1} time")
                continue
        logging.info(f"Failed to insert {article} into database")
        
    def get_article_details(self , url) :

        # for i in range(self.retry) :
        #     try :
        #         res = self.opener.open(url).read()
        #         soup = BeautifulSoup(res, 'html.parser')
        #     except :
        #         logging.info(f"Failed to get news {url} for {i+1} time => Proxy Error !")
        #         continue
        #     if soup.title.string == "Bloomberg - Are you a robot?" :
        #         logging.info(f"Failed to get news {url} for {i+1} time => Mentioned as robot !")
        #         continue
        #     logging.info(f"News detail was scraped in {i+1} time => Successed !")
        #     return {
        #             'publishedAt' : parse(soup.select_one('time').get('datetime')).strftime("%Y-%m-%dT%H:%M:%S.%f%Z") ,
        #             'pulledAt' : datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
        #     } 
            
        # logging.info(f"Failed to get news {url} details => Mentioned as robot ! None was returned !")
        # return {
        #     'publishedAt' : "2000-01-01T00:00:00.000000UTC" ,
        #     'pulledAt' : "2000-01-01T00:00:00.000000UTC"
        # }
        
        z = re.match("https:\/\/www.bloomberg.com\/[A-Za-z0-9]+\/[A-Za-z0-9]+\/([0-9]{4}-[0-9]{2}-[0-9]{2})\/", url)
        if z:
            return {
                    'publishedAt' : z.groups()[0] + "T00:00:00.000000UTC" ,
                    'pulledAt' : datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
            } 
        else :
            return {
                    'publishedAt' : "2000-01-01T00:00:00.000000UTC"  ,
                    'pulledAt' : datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z")
            } 
            
 
    
    def get_articles_of_ticker(self , ticker,insert_to_db = True) :
        for i in range(self.retry) :
            url = f'https://www.bloomberg.com/quote/{ticker}'
            try :
                res = self.opener.open(url).read()
                soup = BeautifulSoup(res, 'html.parser')
            except :
                logging.info(f"Failed to get {url} for {i+1} time => Proxy Error !")
                continue
            if soup.title.string == "Bloomberg - Are you a robot?" :
                logging.info(f"Failed to get {url} for {i+1} time => Mentioned as robot !")
                continue
            try :
                articles = soup.select('article[class*="newsItem__"]')
            except :
                logging.info(f"Failed to get news {url} for {i+1} time => New Items was not found !")
                continue

            try :
                articles = [ { 'title' : item.select_one('div[class*="headline__"]').text , 'url' : item.a.get("href") , 'ticker' : ticker } for item in articles ]
            except :
                logging.info(f"Failed to get news {url} for {i+1} time => News headlines was not found !")
                continue
            
            for index , article in enumerate( articles ) :
                details = self.get_article_details(article['url'])
                articles[index]['publishedAt'] = details['publishedAt']
                articles[index]['pulledAt'] = details['pulledAt']
                if insert_to_db == True :
                    self.insert_article_db(articles[index])
            return articles

    def get_all_articles(self,insert_to_db = True):
        
        articles = []
        for num,ticker in enumerate(self.tickers_list) :
            logging.info(f"Started getting news from {ticker}")
            articles.extend( self.get_articles_of_ticker(ticker) )
        return articles


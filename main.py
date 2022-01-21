from bs4 import BeautifulSoup
import time
import pandas as pd
import os
import warnings
warnings.simplefilter("ignore")
from datetime import datetime
import requests
from urllib.request import Request, urlopen
import re
import logging
import ssl

import logging
logging.basicConfig(filename="scrape_errors.txt",
                   filemode='a',
                   format='%(asctime)s %(levelname)s-%(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S')


def scraper(link):
    ssl._create_default_https_context = ssl._create_unverified_context
    req_base= Request(link,
                   headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'})
    try:
        urlopen(req_base)
        html_base_page = urlopen(req_base)
        soup_base = BeautifulSoup(html_base_page, "lxml")
        text_1=soup_base.text.replace('\n','')
        text_1=text_1.replace('\t','')
        return text_1,soup_base.findAll('a'),soup_base
    except Exception as err:
        logging.error(str(err)+' for link'+str(link))

def scrape_loop(link,level,list_total,college_name,college_url):
    intermediate_data = pd.DataFrame(columns = ['college_name', 'college_url','level_url','base_url''page_url','html_data','cleaned_data','date_stamp'])
    try:
        scraper(link) is not None
        links=scraper(link)[1]
        #print(links)
        duplicate_links=[]
        for link_1 in links:
          if link_1.get('href') is not None and link_1.get('href') not in duplicate_links and link_1.get('href') not in list_total:
            if link_1.get('href').endswith('pdf')==False and link_1.get('href').endswith('jpg')==False and link_1.get('href').endswith('webp')==False:
                if  'https' in link_1.get('href'):
                    try:
                        scraper(link_1.get('href')) is not None
                        print(link_1.get('href'))
                        duplicate_links.append(link_1.get('href'))
                        df={"college_name":college_name,"college_url":college_url,"level_url":level,"base_url":link,"page_url":link_1.get('href'),"html_data":scraper(link_1.get('href'))[2],'cleaned_data':scraper(link_1.get('href'))[0],"date_stamp":datetime.now()}
                        intermediate_data=intermediate_data.append(df,ignore_index=True) 
                    except Exception as err :
                        logging.error(str(err)+' for link ' + str(link_1.get('href')))
        print('end')
        #intermediate_data.to_csv('test_ufv.csv')
        return intermediate_data
    except Exception as err:
        logging.error(str(err)+' for link ' + str(link ))
    
def final_scraping(college_name,college_url,level):
  p=0
  data= [[college_name, college_url,0,college_url,college_url,scraper(college_url)[2],scraper(college_url)[0],datetime.now()]]
  final_data=pd.DataFrame(data,columns = ['college_name', 'college_url','level_url','base_url','page_url','html_data','cleaned_data','date_stamp'])
  while p<=level-2:
    
       print(p)
       list_p=list(final_data[(final_data.level_url==p)]['page_url'])
       for link in list_p:
            list_total=list(final_data['page_url'])
            if 'facebook' not in link and 'twitter' not in link and 'instagram' not in link and 'linked' not in link and 'youtube' not in link:
                print(link)
                print('start')
                
                intermediate_data=scrape_loop(link,p+1,list_total,college_name,college_url)
                final_data=final_data.append(intermediate_data,ignore_index=True)
       p=p+1
  with pd.ExcelWriter('final_scraped_15'+str(college_name)+'.xlsx') as writer:  
        final_data.to_excel(writer, sheet_name='Sheet_name_1')
  return final_data.shape

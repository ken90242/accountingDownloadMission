import http.client
import urllib.parse
import json
import time
import re
from tqdm import tqdm
import sys
import traceback
import pandas as pd
from bs4 import BeautifulSoup
from time import sleep


URL = 'www.salarylist.com'

clips_res = []
alredySavedSlugs = []

df = pd.read_csv('getSalaryCheckList.txt')
# /company/Accenture-Salary.htm?page=14&order=4
for idx, com, page_cur, status in zip(list(df.index), list(df['company']), list(df['page']), list(df['status'])):
  url = '/company/{}?page={}&order=4'.format(com, page_cur)
  if (status != 'done'):
    try:
      df.loc[idx, 'status'] = 'pending'
      df.to_csv('getSalaryCheckList.txt', index=False)

      conn = http.client.HTTPSConnection(URL)
      conn.request(method='GET', url=url)
      r1 = conn.getresponse()
      if (int(r1.status) >= 300 or r1.status < 200):
        raise Exception('{} \nstatus is not 200!'.format(URL + url))
      html_doc = r1.read().decode('utf-8')

      with open('salaryHtmls/{}'.format(url.replace('/company/', '')), 'w') as f:
        f.write(html_doc)
      
      df.loc[idx, 'status'] = 'done'
      df.to_csv('getSalaryCheckList.txt', index=False)

      print('[{}]'.format(r1.status), URL+url)

    except Exception as e:
      traceback.print_exc()
      df.loc[idx, 'status'] = 'fail'
      df.to_csv('getSalaryCheckList.txt', index=False)
      pass



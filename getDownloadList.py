import json
import time
import string
import re
import requests
from tqdm import tqdm
import os.path
import os
import sys
import traceback
import pandas as pd
import pickle as pkl
from bs4 import BeautifulSoup
import multiprocessing as mp
from time import sleep


URL = 'https://www.salarylist.com'

def safeDownload(url):
  RETRY = 20
  retryTimes = RETRY
  while retryTimes > 0:
    try:
      req = requests.get(url)
      if (req.status_code != requests.codes.ok):
        raise Exception('{} \nstatus is not 200!'.format(url))
      return req.content
    except:
      print('[{}/{}] reconnected ... <{}>'.format(RETRY - retryTimes + 1, RETRY, url))
      if (retryTimes == 1): traceback.print_exc()
      retryTimes -= 1

def func(com1stPage):
  try:
    soup = BeautifulSoup(safeDownload(URL + com1stPage), 'html.parser')

    if (soup.find(class_='Google-top') == None):
      raise Exception('{} \nCANNOT find Google-top class!'.format(URL + com1stPage))
    total_amount = int(re.match(r"Total\s+(\d+).+", soup.find(class_='Google-top').getText()).groups()[0])
    total_page = int(total_amount / 20) if (total_amount % 20 == 0) else int(total_amount / 20) + 1
    # time.sleep(3)
  except:
    return (com1stPage.replace('/company/', ''), 0)
  return (com1stPage.replace('/company/', ''), total_page)

"""
  將所有的公司連結parse下來
"""
if __name__ == "__main__":

  if (os.path.exists('totalCompanyHrefs.pickle') == True):
    with open('totalCompanyHrefs.pickle', 'rb') as handle:
      totalCompanyHrefs = pkl.load(handle)
  else:
    totalCompanyHrefs = []

    homeUrl = URL + '/jobs-salary-by-companies.htm'
    soup = BeautifulSoup(safeDownload(homeUrl), 'html.parser')
    nodeLists = soup.find(class_='com-name').find_all('a')
    totalCompanyHrefs += [node.get('href') for node in nodeLists]

    for category in tqdm(['other'] + [a for a in string.ascii_uppercase]):
      alphabetURL = URL + '/jobs-salary-by-companies-{}.htm'.format(category)
      soup = BeautifulSoup(safeDownload(alphabetURL), 'html.parser')
      alphabetPages = len(soup.find(class_='sou-company').find_all('a'))
      for page in range(1, alphabetPages + 1):
        alphabetPageUrl = URL + '/jobs-salary-by-companies-{}{}.htm'.format(category, page)
        soup = BeautifulSoup(safeDownload(alphabetPageUrl), 'html.parser')
        nodeLists = soup.find(class_='com-name').find_all('a')
        totalCompanyHrefs += [node.get('href') for node in nodeLists]

    totalCompanyHrefs = list(set(totalCompanyHrefs))
    
    with open('totalCompanyHrefs.pickle', 'wb') as handle:
      pkl.dump(totalCompanyHrefs, handle, protocol=pkl.HIGHEST_PROTOCOL)

  # totalCompanyHrefs為所有需取得數據的company連結
  print('total unique company: {}'.format(len(totalCompanyHrefs)))

  if (os.path.exists('totalCompanyHrefs.pickle') != True):
    with open('getSalaryCheckList.txt', 'a') as f:
      f.write('company,page,todo\n')

  df = pd.read_csv('getSalaryCheckList.txt')

  # 挑出那些下載為200，但實際上不可用的html
  err_df = pd.read_csv('errorParsingHtmls.log')
  errUrls = err_df['name']
  print('total error Urls: {}'.format(len(errUrls)))
  for errUrl in errUrls:
    # 將壞掉的html移出，並更改downlaod list上的狀態
    if (os.path.exists('salaryHtmls/{}'.format(errUrl)) == True):
      os.rename('salaryHtmls/{}'.format(errUrl), 'errorHtmls/{}'.format(errUrl))
    (company, page) = re.match(r"(.+\.htm)\?page=(\d+).*$", errUrl).groups()
    mask = (df['company'] == company) & (df['page']==int(page))
    df.loc[mask, 'status'] = 'todo'
  df.to_csv('getSalaryCheckList.txt', index=False)

  # 挑出明明有在company list，在download list卻找不到的company
  todoCompany = list(set([href.replace('/company/', '') for href in totalCompanyHrefs]) - set(df['company']))
  print('totalCompanyHrefs', len(totalCompanyHrefs))
  print('salaryCheckList', len(set(df['company'])))
  print('total missing companys', len(todoCompany))
  todoCompany = ['/company/' + i for i in todoCompany]


  processes = mp.Pool()
  with tqdm(total=len(todoCompany)) as pbar:
    for i, res in tqdm(enumerate(processes.imap_unordered(func, todoCompany))):
      (companyUrl, total_page) = res
      for i in range(1, total_page + 1):
        with open('getSalaryCheckList.txt', 'a') as f:
          f.write('{},{},todo\n'.format(companyUrl, i))
      pbar.update()
  pbar.close()
  processes.close()
  processes.join()

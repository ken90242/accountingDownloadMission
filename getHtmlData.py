import os
from bs4 import BeautifulSoup
import multiprocessing as mp
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm
from sys import getsizeof
import re

def colName(idx):
  return {
    0: 'Job_Title',
    1: 'Salaries',
    2: 'City',
    3: 'Year',
    4: 'More_info',
  }[int(idx)]

def parse(filePath):
  company = re.match(r"(.*)\.htm.*", os.path.basename(filePath)).groups()[0]
  with open(filePath) as f:
    soup = BeautifulSoup(f, 'html.parser')
  try:
    trs = soup.find(class_='table_1').find_all('tr')
    df = pd.DataFrame(columns=['Company', 'Job_Title', 'Salaries', 'City', 'Year', 'More_info'])
    df_idx = 0
    for tr in trs[1:]:
      for idx, row in enumerate(tr.find_all('td')):
        df.loc[df_idx, colName(idx)] = row.get_text()
      df_idx += 1
    df['Company'] = company
    return df

  except Exception as ex:
    parse.queue.put((filePath, ex))
    print(ex)
    return pd.DataFrame(columns=['Company', 'Job_Title', 'Salaries', 'City', 'Year', 'More_info'])

def f_init(q):
  parse.queue = q


filePaths = [os.path.join(os.getcwd(), 'salaryHtmls', file) for file in os.listdir('salaryHtmls')]

queue = mp.Queue()

processes = mp.Pool(8, f_init, [queue])

with tqdm(total=len(filePaths)) as pbar:
  dfa = pd.DataFrame(columns=['Company', 'Job_Title', 'Salaries', 'City', 'Year', 'More_info'])
  for i, res in tqdm(enumerate(processes.imap_unordered(parse, filePaths))):
    dfa = dfa.append(res, ignore_index=True)
    # dfa is the bottleneck
    if (i % 500 == 0):
      dfa = dfa[['Company', 'Job_Title', 'Salaries', 'City', 'Year', 'More_info']]
      dfa.to_csv('result_tmp/result_tmp_{}.csv'.format(i), index=False)
      dfa = pd.DataFrame(columns=['Company', 'Job_Title', 'Salaries', 'City', 'Year', 'More_info'])
    pbar.update()
    
  dfa = dfa[['Company', 'Job_Title', 'Salaries', 'City', 'Year', 'More_info']]
  dfa.to_csv('result_tmp/result_tmp_end.csv', index=False)

tmpdfs = [os.path.join(os.getcwd(), 'result_tmp', file) for file in os.listdir('result_tmp')]
dfa = pd.DataFrame(columns=['Company', 'Job_Title', 'Salaries', 'City', 'Year', 'More_info'])

for tmpdf in tmpdfs:
  df = pd.read_csv(tmpdf)
  dfa = dfa.append(df, ignore_index=True)

dfa.to_csv('result.csv', index=False)

with open('errorParsingHtmls.log', 'w') as f:
  f.write('name,error\n')
  while (queue.empty() != True):
    (log, error) = queue.get()
    f.write('{},{}\n'.format(os.path.basename(log),error))

import pandas as pd
import numpy as np
import re
from tqdm import tqdm

df = pd.read_csv('result.csv')

def getMean(a_list):
  return np.mean([int(x.replace(',', '')) for x in a_list])


means = []
lows = []
highs = []
salaryRangeLoss = 0


for salary in tqdm(df['Salaries']):
  twoNumbers = re.match(r"(\d+[^d]*\d*)\-(\d+[^d*]\d+)", salary)
  if (twoNumbers):
    (low, high) = twoNumbers.groups()
    lows.append(int(low.replace(',', '')))
    highs.append(int(high.replace(',', '')))
    means.append(getMean([low, high]))

  else:
    salaryRangeLoss += 1
    lows.append('-')
    means.append('-')
    highs.append('-')

df['Salary_Low'] = lows
df['Salary_High'] = highs
df['Salary_Mean'] = means
df['Company'] = [x.replace('-Salary', '') for x in df['Company']]
df['More_info'] = [x.replace('\n', ';')[:-1] for x in df['More_info']]

df = df[['Company', 'Job_Title', 'Salaries', 'Salary_Low', 'Salary_High', 'Salary_Mean', 'City', 'Year', 'More_info']]
print('salaryRangeLoss:  {}'.format(salaryRangeLoss))
df.to_csv('result_processed.csv', index=False)

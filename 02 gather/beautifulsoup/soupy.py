import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import bs4
# %matplotlib inline
from urllib.request import urlopen
from bs4 import BeautifulSoup

url = "http://www.hubertiming.com/results/2017GPTR10K"
html = urlopen(url)

soup = BeautifulSoup(html, 'lxml')
type(soup)

# output:
# bs4.BeautifulSoup

title = soup.title
print(title)

# Print out the text
#text = soup.get_text()
#print(soup.text)

#soup.find_all('a')

all_links = soup.find_all("a")
for link in all_links:
    print(link.get("href"))

# Print the first 10 rows for sanity check
rows = soup.find_all('tr')
#print(rows[:10])

for row in rows:
    row_td = row.find_all('td')
print(row_td)
type(row_td)

bs4.element.ResultSet

str_cells = str(row_td)
cleantext = BeautifulSoup(str_cells, "lxml").get_text()
print(cleantext)

import re

list_rows = []
for row in rows:
    cells = row.find_all('td')
    str_cells = str(cells)
    clean = re.compile('<.*?>')
    clean2 = (re.sub(clean, '',str_cells))
    list_rows.append(clean2)
print(clean2)
type(clean2)

df = pd.DataFrame(list_rows)
df.head(10)
df1 = df[0].str.split(',', expand=True)
df1.head(10)
df1[0] = df1[0].str.strip('[')
df1.head(10)
col_labels = soup.find_all('th')
all_header = []
col_str = str(col_labels)
cleantext2 = BeautifulSoup(col_str, "lxml").get_text()
all_header.append(cleantext2)
print(all_header)
df2 = pd.DataFrame(all_header)
df2.head()
df3 = df2[0].str.split(',', expand=True)
df3.head()
frames = [df3, df1]
df4 = pd.concat(frames)
df4.head(10)
df5 = df4.rename(columns=df4.iloc[0])
df5.head()
df5.info()
df5.shape
df6 = df5.dropna(axis=0, how='any')
#df6.info()

df6.shape
df7 = df6.drop(df6.index[0])
df7.head()
df7.rename(columns={'[Place': 'Place'},inplace=True)
df7.rename(columns={' Team]': 'Team'},inplace=True)
df7.head()
df7['Team'] = df7['Team'].str.strip(']')
df7.head()

time_list = df7[' Time'].tolist()
time_mins = []

for i in time_list:
    time_parts = i.split(':')
    if len(time_parts) == 3:
        h, m, s = time_parts
    elif len(time_parts) == 2:
        h, m, s = '0', *time_parts
    else:
        print(f"Unexpected time format: {i}")
        continue

    math = (int(h) * 3600 + int(m) * 60 + int(s)) / 60
    time_mins.append(math)

df7['Runner_mins'] = time_mins
df7.head()

df7.describe(include=[np.number])

from pylab import rcParams
rcParams['figure.figsize'] = 15, 5


df7.boxplot(column='Runner_mins')
plt.grid(True, axis='y')
plt.ylabel('Chip Time')
plt.xticks([1], ['Runners'])

x = df7['Runner_mins']
sns.displot(data=df7, x='Runner_mins', kind="hist", kde=True, color='magenta', bins=25, edgecolor='black')
plt.show()

plot_data = df7[df7[' Gender'].isin([' F', ' M'])]
sns.displot(data=plot_data, x='Runner_mins', hue=' Gender', kind='kde', multiple='layer', 
            fill=True, palette={' F': 'magenta', ' M': 'blue'}, common_norm=False)
plt.title('Distribution of Runner Minutes by Gender')
plt.xlabel('Runner Minutes')
plt.ylabel('Density')
plt.legend(title='Gender', labels=['Female', 'Male'])
plt.show()

g_stats = df7.groupby(" Gender", as_index=True).describe()
print(g_stats)

df7.boxplot(column='Runner_mins', by=' Gender')
plt.ylabel('Chip Time')
plt.suptitle("")
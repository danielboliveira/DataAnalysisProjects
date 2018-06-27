from googlesearch import search
import requests
from bs4 import BeautifulSoup

for url in search('neymar',lang='pt-br', stop=1):
    page = requests.get(url)
    
    if (page.status_code != 200):
        print('url:{0} - Erro{1}',url,page.status_code)
        continue
    
    soup = BeautifulSoup(page.content, 'html.parser')
    pars = soup.find_all('p')
    
    for p in pars:
        print(p.get_text())
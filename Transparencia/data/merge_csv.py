import pandas as pd
import os

__path__ = "C:\Cloud\Google Drive\python\Portal da Transparencia\Cartao de Pagamento da Defesa Civil (CPDC)"

files = [f for f in os.listdir(__path__)]

merged = []

for file in files:
    filename, ext = os.path.splitext(file)
    if (ext == '.csv'):
        print(file,end='')
        read = pd.read_csv(__path__ + "\\" + file,sep=';',encoding = "ISO-8859-1", engine='python',quoting=1,quotechar ='"')
        merged.append(read)
        print(' - OK')

result = pd.concat(merged)
result.index.rename('id')
result.to_csv(__path__ + '\\merged.csv',sep=';',encoding = "ISO-8859-1", quoting=1,quotechar ='"')
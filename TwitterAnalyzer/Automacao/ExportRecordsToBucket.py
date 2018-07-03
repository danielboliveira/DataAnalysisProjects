import os,sys
import datetime
import pandas as pd
from SqlServer import dbHelper as db

path,script = os.path.split(sys.argv[0])

try:
    consulta_id = int(sys.argv[1])
except:
    aux = input("Informe o código da consulta: ")
    consulta_id = int(aux)


if ( not os.path.exists(path+'/bucket/')):
    os.mkdir(path+'/bucket/')
    
if ( not os.path.exists(path+'/bucket/' + str(consulta_id))):
    os.mkdir(path+'/bucket/'+ str(consulta_id))

export_path = path+'/bucket/'+ str(consulta_id) +'/'
tws_filename = export_path +'data.export'
print(tws_filename)

#df = pd.read_hdf(tws_filename,key='tws')
#print(df.count())

cr,cnxn = db.getConnection()
cr.execute('select * from twitter where consulta_id = ?',consulta_id)
df = pd.DataFrame(cr.fetchall())
df.to_hdf(tws_filename,key='tws')

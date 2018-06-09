# -*- coding: utf-8 -*-
import DataAccess.Twitters
import Helpers.Utils
import datetime
from bson.objectid import ObjectId
import pyodbc
import sys

cnxn = pyodbc.connect('DSN=sqlTwitter;uid=sa;PWD=sa')
cursor = cnxn.cursor()
log = open('import.log', 'a')

def writeLog(mensagem):
    now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    log.write('{0}-{1}\n'.format(now,mensagem))

def importPlace(place):
    cmdSelect = 'select count(*) from place where id = ?'
    cmdInsert = 'INSERT INTO [place]'\
              '([id],[url],[place_type],[name],[full_name],[country_code],[country],[bounding_box])'\
              'VALUES (?,?,?,?,?,?,?,?)'
    #verifica se já existe o place
    id = place['id']
    cursor.execute(cmdSelect,id)
    result = cursor.fetchone()

    if(result[0] > 0):
        return
    
    url = place['url']
    place_type = place['place_type']
    name = place['name']
    full_name = place['full_name']
    country_code = place['country_code']
    country = place['country']
    bounding_box = None
    cursor.execute(cmdInsert,id,url,place_type,name,full_name,country_code,country,bounding_box)
    
try:
    writeLog('Inicio do processo')
    print('Inicio do processo')
    
    post = DataAccess.Twitters.twitters.find_one({'place':{'$ne':None}})
    
    if ('place' in post and post['place'] != None):
        importPlace(post['place'])
        cursor.commit()
    
except Exception as e:
    writeLog('Erro:{0}'.format(str(e)))
finally:
    cursor.commit()
    cursor.close()
    writeLog('Processo finalizado')

print('Fim do processo')
    
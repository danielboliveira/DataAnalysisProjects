ãoimport pyodbc
import Helpers.Utils
import datetime
import twython
import pickle
import os
import time

cnxn = pyodbc.connect('DSN=sqlTwitter;uid=sa;PWD=sa',autocommit=True)
cursor = cnxn.cursor()

cmdInsertRt = 'insert retwitted_process (twitter_id,level,twitter_id_original,processed) '\
              'values(?,?,?,?)'

def getNextLevel(post_id,level):
    cmd ='select a.id from twitter a where retweeted_status_id = ? '\
         'and not exists(select 1 from retwitted_process rp where rp.twitter_id = a.id)'
    cursor.execute(cmd,post_id)
    data = cursor.fetchall()

    for post in data:
        cursor.execute(cmdInsertRt,
                   post.id,
                   level + 1,
                   post_id,
                   datetime.datetime.now())        
        cursor.commit()
        getNextLevel(post.id,level + 1)

cmdUpdateApiProcessed = 'update retwitted_process set api_find_processed = 1 where twitter_id = ?'
#Obter os Twitters IDs que foram compartilhados
cmdRoot ='select a.id from twitter a where retweeted_status_id is null and exists(select 1 from twitter tw where tw.retweeted_status_id = a.id) and not exists(select 1 from retwitted_process rtw where rtw.twitter_id = a.id) order by 1'
cursor.execute(cmdRoot)
data = cursor.fetchall()
total = len(data)
index = 0

print("*** Inserindo tws roots ***")
if total > 0:
    
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
    for post in data:
        #insere os post roots
        cursor.execute(cmdInsertRt,
                       post.id,
                       0,
                       None,
                       datetime.datetime.now())
        cursor.execute(cmdUpdateApiProcessed,post.id)
    #    getNextLevel(post.id,0)
        index += 1
        Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
    cursor.commit()


print("*** Percorrendo tws roots ***")

cmdRootProcess ='select twitter_id from retwitted_process where twitter_id_original is null and [api_find_processed] = 0'
cursor.execute(cmdRootProcess)
data = cursor.fetchall()
total = len(data)

if total > 0:
    index = 0
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
    for post in data:
        getNextLevel(post.twitter_id,0)
        index += 1
        Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
    cursor.commit()
    
print("*** Atualizando demais rts ****")
cmdQuery ='select twitter_id from retwitted_process where level > 0 and [api_find_processed] = 0'

 
cursor.execute(cmdQuery)
data = cursor.fetchall()
total = len(data)

if total > 0:
    cfg = Helpers.Utils.getConfig('crawler.config')
    tw = twython.Twython(cfg['CONSUMER_KEY'],cfg['CONSUMER_SECRET'],
                         cfg['ACCESS_TOKEN'],cfg['ACCESS_TOKEN_SECRET'])
    index = 0
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
    for post in data:
        post_id = post.twitter_id
        try:
            rts = tw.get_retweets(id=post_id)
            
            if (rts == None or len(rts) <= 0):
                continue
            
            filename =  './retweets/{0}.pkl'.format(post_id)
            if os.path.exists(filename):
                os.remove(filename)
        
            output = open(filename, 'wb')
            pickle.dump(rts, output, -1)
            output.close() 
            
            cursor.execute(cmdUpdateApiProcessed,post_id)
            cursor.commit()
            time.sleep(1000)
            
        except Exception as e:
            print("\nErro na obtenção da consulta a API:{0}".format(str(e)))
            break
    


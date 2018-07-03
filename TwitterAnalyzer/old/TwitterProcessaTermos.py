import DataAccess.Twitters
import Helpers.Utils
import datetime
from bson.objectid import ObjectId
import pyodbc
import sys

twitters = DataAccess.Twitters.twitters
search = 'seleção brasileira'
reprocess = False

cnxn = pyodbc.connect('DSN=sqlTwitter;uid=sa;PWD=sa')
cursor = cnxn.cursor()


if (reprocess):
    DataAccess.Twitters.removeTermoAnalyzed(search)
#    DataAccess.Twitters.removeTermo(search)    

consulta = DataAccess.Twitters.getTwittersByTermoForAnalyzer(search)

total = consulta.count()
index = 0
Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

fila = dict()
log = open(search + '.log', 'w')

for r in consulta:
    aux = []
    text = search    

    criacao = r['criacao']
    citacao = datetime.datetime(criacao.year,criacao.month,criacao.day,criacao.hour,0)     
    post_id = r['id']
    
    userid = r['user']['id']
    username = r['user']['name']
    userlocation = r['user']['location']
    
    if (userlocation != None and len(userlocation) > 100):
        userlocation = userlocation[0:100]
        
    
    lang = r['lang']
    place = r['place']
    
    placename = None
    placefullname = None
    placecountrycode = None
    placecountry = None
    placetype = None
    
    if (place):
        placename = place['name']
        placefullname = place['full_name']
        placecountrycode = place['country_code']
        placecountry = place['country']
        placetype = place['place_type']
    
    hashtags = ''
    if (r['entities']):
         for  tag in r['entities']['hashtags']:
            hashtags = hashtags + ',' + tag['text']
              
        
    quote_count = r['quote_count']
    reply_count = r['reply_count']
    retweet_count = r['retweet_count']
    favorite_count = r['favorite_count']        
    retweeted = r['retweeted']
    negativo = r['negativo']
    positivo = r['positivo']
    sentimento = r['sentimento']
    
    entidades = ""
    
    if ('entidades' in r):
        for ent in r['entidades']:
            if (str(ent).lower().find(text) == -1):
                entidades = entidades + ',' + ent
    
    
    
           
            
#    termo = {'id':post_id,
#             'termo':text,
#             'criacao':criacao,
#             'citacao':citacao,
#             'userid':userid,
#             'username':username,
#             'userlocation':userlocation,
#             'lang':lang,
#             'placename':placename,
#             'placefullname':placefullname,
#             'placecountrycode':placecountrycode,
#             'placecountry':placecountry,
#             'placetype':placetype,
#             'hashtags':hashtags,
#             'quotecount':quote_count,
#             'replycount':reply_count,
#             'retweetcount':reply_count,
#             'favoritecount':favorite_count,
#             'retweeted':retweeted,
#             'positivo':positivo,
#             'negativo':negativo,
#             'sentimento':sentimento,
#             'entidades':entidades}
    try:
    
        cursor.execute(r'INSERT INTO [dbo].[termos]'\
               '([id]           ,[termo]          ,[criacao]     ,[citacao]         ,[userid]           ,[username]'\
               ',[userlocation] ,[lang]           ,[placename]   ,[placefullname]   ,[placecountrycode]'\
               ',[placecountry] ,[placetype]      ,[hashtags]    ,[quotecount]      ,[replycount]'\
               ',[retweetcount] ,[favoritecount]  ,[retweeted]   ,[positivo]        ,[negativo]'\
               ',[sentimento]   ,[entidades])'\
               'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', 
                post_id,text,criacao,citacao,userid,username,
                userlocation,lang,placename,placefullname,placecountrycode,      
                placecountry,placetype,hashtags,quote_count,reply_count,
                reply_count,favorite_count,retweeted,positivo,negativo,
                sentimento,entidades)

        #Atualiza o controle de termos analizados
        if ('entidades_analyzed' in r):
            aux = r['entidades_analyzed']
        if (aux == None):
            aux = []
        aux.append({'tag':text})
               
        fila[r['_id']] = aux
        
        if (index == 0 or index % 1000 == 0):
            try:
                cnxn.commit()
            
                for key in fila:
                    twitters.update_one({'_id':ObjectId(key)},{"$set":{'entidades_analyzed':fila[key]}},upsert=False)
            
                fila.clear()
            except:
                log.write("COMMIT - {0} - {1}\n".format(index,sys.exc_info()[0]))
                for key in fila:
                    log.write("\t{0}\n".format(fila[key]))
                log.flush()    
                raise
    except:
        log.write("{0}\n".format(sys.exc_info()[0]))
        log.flush()
        raise
    finally:
        cursor.commit()
    
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    index+=1

cursor.commit()
cursor.close()
log.close()
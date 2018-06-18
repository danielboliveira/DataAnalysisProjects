# -*- coding: utf-8 -*-
import DataAccess.Twitters
import Helpers.Utils
import datetime
import pyodbc
import pickle
import os
import unicodecsv as csv
import sys

__cnxn = pyodbc.connect('DSN=sqlTwitter;uid=sa;PWD=sa',autocommit=True)
__cursor = __cnxn.cursor()
__path_not_processed = './crawler_results_not_process/'
__log = open('import.log', 'a')
__verbose = True

__root_csv_path  ='./crawler_results/bulkinsert/'

### Conteners para os dados a serem exportados ###
__places = dict()
__entidades = dict()
__hashtags = dict()
__geo = dict()
__users = dict()
__posts = dict()

def InitConteners():
    global __places
    global __entidades
    global __hashtags
    global __geo
    global __users
    
    __places.clear()
    __entidades.clear()
    __hashtags.clear()
    __geo.clear()
    __users.clear()
    __posts.clear()
    
def SaveDicts(FileToImport):
    fileNameRoot = __root_csv_path + os.path.splitext(FileToImport)[0]
    __save(fileNameRoot+'_places.csv',__places)
    __save(fileNameRoot+'_entidades.csv',__entidades)
    __save(fileNameRoot+'_hashtags.csv',__hashtags)
    __save(fileNameRoot+'_geo.csv',__geo)
    __save(fileNameRoot+'_users.csv',__users)
    InitConteners()

def __save(fileName,collection):
    csv.register_dialect('myDialect',delimiter = ';',quoting=csv.QUOTE_NONE,skipinitialspace=True)    
    
    f = open(fileName, 'w')
    writer = csv.writer(f, dialect='myDialect', encoding='utf-8')
    try:
        for tupla in collection.values():
            writer.writerow(tupla)
    except Exception as e:
        writer.writerow("Erro:{0}".format(e))
    finally:
        f.close()
    
def InitConnection():
    global __cnxn
    global __cursor
    __cnxn = pyodbc.connect('DSN=sqlTwitter;uid=sa;PWD=sa',autocommit=True)
    __cursor = __cnxn.cursor()

def Commit():
    global __cursor
    __cursor.commit()
    
def CloseConnection():
    try:
        global __cursor
        __cursor.close()
    except:
        pass

def FlushLog():
    __log.flush()
    
     
def __writeLog(mensagem):
    if (not (__verbose)):
        return
    try:
        now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        __log.write('{0}-{1}\n'.format(now,mensagem))
        __log.flush()
    except:
        pass
    
def __importPlace(place):
    
    url = place['url']
    place_type = place['place_type']
    name = place['name'].replace('"','')
    full_name = place['full_name'].replace('"','')
    country_code = place['country_code']
    country = place['country'].replace('"','')
    bounding_box = None
    
    #__cursor.execute(cmdInsert,id,url,place_type,name,full_name,country_code,country,bounding_box)
    global __places
    if not(id in __places):
        __places[id] = (id,url,place_type,name,full_name,country_code,country,bounding_box)
        
    
def __importEntidades(id,ents):
    for ent in ents:
        id_hash = hash(str(id)+ent)
        if not (id_hash in __entidades):
            __entidades[id_hash] = (id,ent)
            

def __importHashtags(id,hashtags):
    for hashtag in hashtags:
        id_hash = hash(str(id)+hashtag['text'])
        if not(id_hash in __hashtags):
            __hashtags[id_hash] = (id,hashtag['text'])
    

def __importGeo(id,geo):
    
    id_hash = hash(str(id)+str(geo['type'])+str(geo['coordinates'][0])+str(geo['coordinates'][1]))    
    if not(id_hash in __geo):
        __geo[id_hash] = (id,geo['type'],geo['coordinates'][0],geo['coordinates'][1],id)

def __importUser(user):
    id	= user['id']
    
    #Verifica a existência do User
    if (id in __users):
        return;
      
    name 					= user['name']
    screen_name 			= user['screen_name']
    location 				= user['location']
    url 					= user['url']
    description 			= user['description']
    protected 				= user['protected']
    verified 				= user['verified']
    followers_count		 	= user['followers_count']
    friends_count 			= user['friends_count']
    listed_count 			= user['listed_count']
    favourites_count		= user['favourites_count']
    statuses_count 			= user['statuses_count']
    created_at 				= Helpers.Utils.convertTwitterUTCTimeToLocal(user['created_at'])
    geo_enabled 			= user['geo_enabled']
    time_zone 				= user['time_zone']
    lang 					= user['lang']
    contributors_enabled 	= user['contributors_enabled']
    default_profile 		= user['default_profile']
    
    withheld_in_countries = None
    withheld_scope = None
    
    if ('withheld_in_countries' in user and user['withheld_in_countries'] != None):
        withheld_in_countries 	= user['withheld_in_countries']
    
    if ('withheld_scope' in user and user['withheld_scope'] != None):
        withheld_scope 			= user['withheld_scope']
    
    
    if (location != None and len(location) > 200):
        location = location[0:200]
    
    if (description != None and len(description) > 8000):
        description = description[0:8000]
        
    __users[id] = ( id 						, 
                    name 					, 
                    screen_name 			, 
                    location 				, 
                    url 					, 
                    description 			, 
                    protected 				, 
                    verified 				, 
                    followers_count		 	, 
                    friends_count 			, 
                    listed_count 			, 
                    favourites_count		, 
                    statuses_count 			, 
                    created_at 				, 
                    geo_enabled 			, 
                    time_zone 				, 
                    lang 					, 
                    contributors_enabled 	, 
                    default_profile 		, 
                    withheld_in_countries 	, 
                    withheld_scope 			)
    
                   
def __importPost(post):
    
    if (post['id'] in __posts):
        return
    
    if ('place' in post and post['place'] != None):
        __importPlace(post['place'])
    
    if ('entidades' in post and post['entidades'] != None):
        __importEntidades(post['id'],post['entidades'])
        
    if ('entities' in post and post['entities'] != None):
        if ('hashtags' in post['entities'] and post['entities']['hashtags'] != None):
            __importHashtags(post['id'],post['entities']['hashtags'])
            
    if ('geo' in post and post['geo'] != None):
        __importGeo(post['id'],post['geo'])
    
    __importUser(post['user'])

    created_at 					= 	Helpers.Utils.convertTwitterUTCTimeToLocal(post['created_at'])
    id							= 	post['id']					
    text 						=	post['text']
    source 					  	=	post['source']
    truncated 					=	post['truncated']
    in_reply_to_status_id 		=	post['in_reply_to_status_id']
    in_reply_to_user_id 		=	post['in_reply_to_user_id']
    in_reply_to_screen_name 	=	post['in_reply_to_screen_name']
    user_id						=	post['user']['id']
    
    place_id = None
    if ('place' in post and post['place'] != None):
        place_id =	post['place']['id']
    
    is_quote_status 			=	post['is_quote_status']
    quote_count 				=	post['quote_count']
    reply_count 				=	post['reply_count']
    retweet_count 				=	post['retweet_count']
    favorite_count 				=	post['favorite_count']
    favorited 					=	post['favorited']
    retweeted 					=	post['retweeted']
    
    possibly_sensitive = None
    if ('possibly_sensitive' in post):
        possibly_sensitive 			=	post['possibly_sensitive']
    
    filter_level = None
    if ('filter_level' in post):
        filter_level =	post['filter_level']
    
    lang =	post['lang']
    
    if ('negativo' in post):
        negativo =	post['negativo']
    else:
        negativo = None
    
    if ('positivo' in post):    
        positivo =	post['positivo']
    else:
        positivo = None
    
    if ('sentimento' in post):    
        sentimento 	=	post['sentimento']
    else:
        sentimento = None
    
    quoted_status_id = None
    
    if ('quoted_status' in post and post['quoted_status'] != None):
        quoted_status_id = post['quoted_status']['id']
        __importPost(post['quoted_status'])
    
    
    retweeted_status_id = None
    if ('retweeted_status' in post and post['retweeted_status'] != None):
        retweeted_status_id = post['retweeted_status']['id']
        __importPost(post['retweeted_status'])
    
  
    __posts[id] = (created_at 					,             id							, 
            text 						,            source 					  	,
            truncated 					,            in_reply_to_status_id 		,
            in_reply_to_user_id 		,            in_reply_to_screen_name 	,
            user_id						,            place_id                    ,	
            quoted_status_id            ,                      
            is_quote_status 			,            retweeted_status_id,
            quote_count 				,            reply_count 				,
            retweet_count 				,            favorite_count 				,
            favorited 					,            retweeted 					,
            possibly_sensitive 			,            filter_level 				,
            lang 					 	,            negativo 					,
            positivo 					,            sentimento)


def importToSqlCollectionMongoDB():
    
    to_commit = []      
    time = datetime.datetime.now()
     
    try:
        __writeLog('########### Inicio do processo ###########')
        print('Inicio do processo')
    
        consulta = DataAccess.Twitters.twitters.find({'fl_sql_migrated' : False })
        total = consulta.count()
        index = 0

        Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    
        for post in consulta:
           __importPost(post)
           to_commit.append(post['id'])
        
           Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
           index += 1
        
           if (index == 0 or index % 1000 == 0):
            try:
                __writeLog('Tentando commit')
                __cursor.commit()
                __writeLog('Commit realizado')
                
                time_aux = time
                time = datetime.datetime.now()
                minutes_diff = (time - time_aux).total_seconds() / 60.0
                
                __writeLog('Total de Minutos(s):{0}'.format(minutes_diff))
                
                for id in to_commit:
                    DataAccess.Twitters.twitters.delete_one({'id':id})
    
            except Exception as e:
                filename = post['id']+'.post'
                output = open(filename, 'wb')
                pickle.dump(post, output, pickle.HIGHEST_PROTOCO)
                output.close() 
                msg = 'Erro:{0}'.format(str(e))
                __writeLog(msg)
            finally:
                __writeLog(",".join([str(i) for i in to_commit]))    
                to_commit.clear()
    
    except Exception as e:
        msg = 'Erro:{0}'.format(str(e))
        __writeLog(msg)
        __writeLog(",".join([str(i) for i in to_commit]))  
        print('\n'+msg)
    finally:
        __cursor.commit()
        __cursor.close()
        __writeLog('######## Processo finalizado #######\n')
        __log.flush()
        __log.close()         
        print('Fim do processo')

def importToSql(post,verbose=True):
    global __verbose
    __verbose = verbose
    try:
       __importPost(post)
    except pyodbc.ProgrammingError as e:
       filename = __path_not_processed + '{0}.post'.format(post['id'])
       output = open(filename, 'wb')
       pickle.dump(post, output, pickle.HIGHEST_PROTOCOL)
       output.close() 
       msg = 'Erro:{0}'.format(str(e))
       print(msg)
#    finally:
#      __cursor.commit()
   
        
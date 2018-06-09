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
    cursor.commit()
    
def importEntidades(id,ents):
    cmdDelete = 'delete from entidade where twitter_id = ?'
    cursor.execute(cmdDelete,id)
    
    for ent in ents:
        cmdInsert = 'insert into entidade (twitter_id,text) values(?,?)'
        cursor.execute(cmdInsert,id,ent)
        
    cursor.commit()

def importHashtags(id,hashtags):
    cursor.execute('delete from [hashtag] where twitter_id = ?',id)
    cursor.commit()
    
    for hashtag in hashtags:
        cursor.execute('insert into hashtag (twitter_id,text) values(?,?)',id,hashtag['text'])
    
    cursor.commit()

def importGeo(id,geo):
    cursor.execute('delete from geo where twitter_id = ?',id)
    cursor.commit()
    
    cursor.execute('insert into geo (type,longitude,latitude,twitter_id) values (?,?,?,?)',
                   geo['type'],
                   geo['coordinates'][0],
                   geo['coordinates'][1],
                   id)
    cursor.commit()

def importUser(user):
    id 						= user['id']
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
    
    cursor.execute('delete from [user] where id = ?',id)
    cursor.commit()
    
    if (len(location) > 200):
        location = location[0:200]
    
    if (len(description) > 8000):
        description = description[0:8000]
        
    cursor.execute('INSERT INTO [user] ([id],[name],[screen_name],[location],[url],[description]'\
				   ',[protected],[verified],[followers_count],[friends_count]'\
                   ',[listed_count],[favourites_count],[statuses_count],[created_at]'\
				   ',[geo_enabled],[time_zone],[lang],[contributors_enabled],[default_profile]'\
				   ',[withheld_in_countries],[withheld_scope])'\
                   'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    id 						, 
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
    cursor.commit()
                   
def importPost(post):
    
    cursor.execute('select count(*) from twitter where id=?',post['id'])
    result = cursor.fetchone()

    if(result[0] > 0):
        return
    
    if ('place' in post and post['place'] != None):
        importPlace(post['place'])
    
    if ('entidades' in post and post['entidades'] != None):
        importEntidades(post['id'],post['entidades'])
        
    if ('entities' in post and post['entities'] != None):
        if ('hashtags' in post['entities'] and post['entities']['hashtags'] != None):
            importHashtags(post['id'],post['entities']['hashtags'])
            
    if ('geo' in post and post['geo'] != None):
        importGeo(post['id'],post['geo'])
    
    importUser(post['user'])

    created_at 					= 	Helpers.Utils.convertTwitterUTCTimeToLocal(post['created_at'])
    id							= 	post['id']					
    text 						=	post['text']
    source 					  	=	post['source']
    truncated 					=	post['truncated']
    in_reply_to_status_id 		=	post['in_reply_to_status_id']
    in_reply_to_user_id 		=	post['in_reply_to_user_id']
    in_reply_to_screen_name 	=	post['in_reply_to_screen_name']
    user_id						=	post['user']['id']
    
    if ('place' in post and post['place'] != None):
        place_id =	post['place']['id']
    
    is_quote_status 			=	post['is_quote_status']
    quote_count 				=	post['quote_count']
    reply_count 				=	post['reply_count']
    retweet_count 				=	post['retweet_count']
    favorite_count 				=	post['favorite_count']
    favorited 					=	post['favorited']
    retweeted 					=	post['retweeted']
    possibly_sensitive 			=	post['possibly_sensitive']
    filter_level 				=	post['filter_level']
    lang 					 	=	post['lang']
    negativo 					=	post['negativo']
    positivo 					=	post['positivo']
    sentimento 					=	post['sentimento']
    
    quoted_status_id = None
    quoted_status_text = None
    retweeted_status_id = None
    
    cursor.execute('INSERT INTO [twitter]'\
           '([created_at],[id],[text],[source],[truncated],[in_reply_to_status_id],[in_reply_to_user_id]'\
           ',[in_reply_to_screen_name],[user_id],[place_id],[quoted_status_id],[quoted_status_text]'\
           ',[is_quote_status],[retweeted_status_id],[quote_count],[reply_count],[retweet_count]'\
           ',[favorite_count],[favorited],[retweeted],[possibly_sensitive],[filter_level]'\
           ',[lang],[negativo],[positivo],[sentimento])'\
           ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            created_at 					,             id							, 
            text 						,            source 					  	,
            truncated 					,            in_reply_to_status_id 		,
            in_reply_to_user_id 		,            in_reply_to_screen_name 	,
            user_id						,            place_id                    ,	
            quoted_status_id            ,            quoted_status_text          ,
            is_quote_status 			,            retweeted_status_id,
            quote_count 				,            reply_count 				,
            retweet_count 				,            favorite_count 				,
            favorited 					,            retweeted 					,
            possibly_sensitive 			,            filter_level 				,
            lang 					 	,            negativo 					,
            positivo 					,            sentimento)

### Código Principal ####    
try:
    writeLog('########### Inicio do processo ###########\n')
    print('Inicio do processo')
    
    post = DataAccess.Twitters.twitters.find_one({ "geo" : {'$ne':None} })
    
    importPost(post) 
    cursor.commit()
    
except Exception as e:
    msg = 'Erro:{0}'.format(str(e))
    writeLog(msg)
    print(msg)
finally:
    cursor.commit()
    cursor.close()
    writeLog('######## Processo finalizado #######\n')

print('Fim do processo')
    
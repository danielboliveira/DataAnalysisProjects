# -*- coding: utf-8 -*-
import Helpers.Utils
import datetime
import pyodbc
import SqlServer.dbHelper as db
#import db
import pickle

__path_not_processed = './crawler_results_not_process/'

__log = open('import.log', 'a')
__verbose = True


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
    cmdSelect = 'select count(*) from place where id = ?'
    cmdInsert = 'INSERT INTO [place]'\
              '([id],[url],[place_type],[name],[full_name],[country_code],[country],[bounding_box])'\
              'VALUES (?,?,?,?,?,?,?,?)'
    #verifica se já existe o place
    id = place['id']
    
    __cursor,_ = db.getConnection()
    __cursor.execute(cmdSelect,id)
    result = __cursor.fetchone()

    if(result[0] > 0):
        return
    
    url = place['url']
    place_type = place['place_type']
    name = place['name']
    full_name = place['full_name']
    country_code = place['country_code']
    country = place['country']
    bounding_box = None
    __cursor.execute(cmdInsert,id,url,place_type,name,full_name,country_code,country,bounding_box)
    
   
def __importEntidades(id,ents):
    
    cmdDelete = 'delete from entidade where twitter_id = ?'
    __cursor,_ = db.getConnection()
    __cursor.execute(cmdDelete,id)
    
    for ent in ents:
        cmdInsert = 'insert into entidade (twitter_id,text) values(?,?)'
        __cursor.execute(cmdInsert,id,ent)
    
#    writeLog('Entidades - {0} - Tentando commit'.format(id))    
#    cursor.commit()

def __importHashtags(id,hashtags):
    __cursor,_ = db.getConnection()
    __cursor.execute('delete from [hashtag] where twitter_id = ?',id)
#    cursor.commit()
    
    for hashtag in hashtags:
        __cursor.execute('insert into hashtag (twitter_id,text) values(?,?)',id,hashtag['text'])
    
#    writeLog('HashTags - {0} - Tentando commit'.format(id))
#    cursor.commit()

def __importGeo(id,geo):
    __cursor,_ = db.getConnection()
    __cursor.execute('delete from geo where twitter_id = ?',id)
#    cursor.commit()
    
    __cursor.execute('insert into geo (type,longitude,latitude,twitter_id) values (?,?,?,?)',
                   geo['type'],
                   geo['coordinates'][0],
                   geo['coordinates'][1],
                   id)
    
#    writeLog('Geo - {0} - Tentando commit'.format(id))
#    cursor.commit()

def __importUser(user):
    id	= user['id']
    
    #Verifica a existência do User
    __cursor,_ = db.getConnection()
    __cursor.execute('select count(*) from [user] where id = ?',id)
    result = __cursor.fetchone()
    userExists = (result[0] > 0) #Criar a rotina para o Update
    
    if (userExists):
        return
      
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
    
#    cursor.execute('delete from [user] where id = ?',id)
#    cursor.commit()
    
    if (location != None and len(location) > 200):
        location = location[0:200]
    
    if (description != None and len(description) > 8000):
        description = description[0:8000]
        
    if (userExists):
        __cursor.execute('SELECT [id],[name],[screen_name],[location],[url],[description],[protected],[verified],[followers_count],[friends_count]'\
                               ',[listed_count],[favourites_count],[statuses_count],[created_at],[geo_enabled],[time_zone],[lang],[contributors_enabled]'\
                               ' ,[default_profile],[withheld_in_countries],[withheld_scope]'\
                               'FROM [dbo].[user] where id = ?',
                               id)
        row = __cursor.fetchone()[0]
        if (row == None):
            return;
        
#        Somente faz o update se for necessário
        
        update = ((name != row.name) or (screen_name != row.screen_name) or (location != row.location) or
                 (url != row.url) or (description != row.description) or (protected != row.protected) or
                 (verified != row.verified) or (followers_count != row.followers_count) or (friends_count != row.friends_count) or
                 (friends_count != row.friends_count) or (listed_count != row.listed_count) or (favourites_count != row.favourites_count) or
                 (statuses_count != row.statuses_count) or (created_at != row.created_at) or (geo_enabled != row.geo_enabled) or (time_zone != row.time_zone) or
                 (lang != row.lang) or (contributors_enabled != row.contributors_enabled) or (default_profile != row.default_profile) or 
                 (withheld_in_countries != row.withheld_in_countries) or (withheld_scope != row.withheld_scope))
                
        if (update):
            __cursor.execute('UPDATE [dbo].[user]'\
                                 'SET [name] = ?'\
                                 ',[screen_name] = ?'\
                                 ',[location] = ?'\
                                 ',[url] = ?'\
                                 ',[description] = ?'\
                                 ',[protected] = ?'\
                                 ',[verified] = ?'\
                                 ',[followers_count] = ?'\
                                 ',[friends_count] = ?'\
                                 ',[listed_count] = ?'\
                                 ',[favourites_count] = ?'\
                                 ',[statuses_count] = ?'\
                                 ',[created_at] = ?'\
                                 ',[geo_enabled] = ?'\
                                 ',[time_zone] = ?'\
                                 ',[lang] = ?'\
                                 ',[contributors_enabled] = ?'\
                                 ',[default_profile] = ?'\
                                 ',[withheld_in_countries] = ?'\
                                 ',[withheld_scope] = ?'\
                                 ' WHERE [id] = ?',
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
                    withheld_scope 	        ,
                    id 						 )
            
    else:
        
        __cursor.execute('INSERT INTO [user] ([id],[name],[screen_name],[location],[url],[description]'\
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
    
#    writeLog('user - {0} - Tentando commit'.format(id))
#    cursor.commit()
                   
def __importPost(post,tags,consulta_id):
    
    __cursor,_ = db.getConnection()
    __cursor.execute('select count(*) from twitter where id=?',post['id'])
    result = __cursor.fetchone()

    if(result[0] > 0):
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
        __importPost(post['quoted_status'],tags,consulta_id)
    
    
    retweeted_status_id = None
    if ('retweeted_status' in post and post['retweeted_status'] != None):
        retweeted_status_id = post['retweeted_status']['id']
        __importPost(post['retweeted_status'],tags,consulta_id)
    
  
      
    __cursor.execute('INSERT INTO [twitter]'\
           '([created_at],[id],[text],[source],[truncated],[in_reply_to_status_id],[in_reply_to_user_id]'\
           ',[in_reply_to_screen_name],[user_id],[place_id],[quoted_status_id]'\
           ',[is_quote_status],[retweeted_status_id],[quote_count],[reply_count],[retweet_count]'\
           ',[favorite_count],[favorited],[retweeted],[possibly_sensitive],[filter_level]'\
           ',[lang],[negativo],[positivo],[sentimento],consulta_id,tags)'\
           ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            created_at 					,             id							, 
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
            positivo 					,            sentimento,consulta_id,tags)


def importToSql(post,tags,consulta_id,verbose=True):
    global __verbose
    __verbose = verbose
    
    try:
       __importPost(post,tags,consulta_id)
    except pyodbc.ProgrammingError as e:
       filename = __path_not_processed + '{0}.post'.format(post['id'])
       output = open(filename, 'wb')
       pickle.dump(post, output, pickle.HIGHEST_PROTOCOL)
       output.close() 
       msg = 'Erro:{0}'.format(str(e))
       print(msg)
        
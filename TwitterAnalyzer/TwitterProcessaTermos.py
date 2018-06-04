import DataAccess.Twitters
import Helpers.Utils
import datetime
from bson.objectid import ObjectId

twitters = DataAccess.Twitters.twitters
search = 'bolsonaro'
reprocess = False

if (reprocess):
    DataAccess.Twitters.removeTermoAnalyzed(search)
    DataAccess.Twitters.removeTermo(search)    

consulta = DataAccess.Twitters.getTwittersByTermoForAnalyzer(search)

total = consulta.count()
index = 0
Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

for r in consulta:
    aux = []
    text = search    

    criacao = r['criacao']
    citacao = datetime.datetime(criacao.year,criacao.month,criacao.day,criacao.hour,0)     
    post_id = r['id']
    
    isreply = False
    replyuserid = None
    replyusername = None
    
    if ('in_reply_to_status_id' in r) and (r['in_reply_to_status_id'] != None):
        isreply =  True
    if ('in_reply_to_user_id' in r):
        replyuserid = r['in_reply_to_user_id']
    if ('in_reply_to_screen_name' in r):
        replyusername = r['in_reply_to_screen_name']
    
    userid = r['user']['id']
    username = r['user']['name']
    userlocation = r['user']['location']
    followerscount = r['user']['followers_count']
    friendscount = r['user']['friends_count']
    lang = r['user']['lang']
    following = r['user']['following']
    
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
    
    hashtags = None    
    if (r['entities']):
        hashtags = r['entities']['hashtags']
        
    quote_count = r['quote_count']
    reply_count = r['reply_count']
    retweet_count = r['retweet_count']
    favorite_count = r['favorite_count']        
    favorited = r['favorited']
    retweeted = r['retweeted']
    negativo = r['negativo']
    positivo = r['positivo']
    sentimento = r['sentimento']
    
    entidades = []
    
    if ('entidades' in r):
        for ent in r['entidades']:
            if (str(ent).lower().find(text) == -1):
                entidades.append(ent)
    
    
    usercreated = None
    retweet_criacao = None
    retweet_id = None
    retweetuser_id = None
    retweetuser_location = None
    retweetuser_followers_count = None
    retweetuser_friends_count = None
    retweetuser_created_at = None
    retweetuser_following = None
    
    if ('created_at' in r['user']):
      usercreated = Helpers.Utils.convertTwitterUTCTimeToLocal(r['user']['created_at'])
    
    if ('retweeted_status' in r) and (r['retweeted_status'] != None) :
        retweet_criacao = Helpers.Utils.convertTwitterUTCTimeToLocal(r['retweeted_status']['created_at'])
        retweet_id  = r['retweeted_status']['id']
        
        if ('user' in r['retweeted_status']):
           retweetuser_id =  r['retweeted_status']['user']['id']
           retweetuser_location =  r['retweeted_status']['user']['location']
           retweetuser_followers_count =  r['retweeted_status']['user']['followers_count']
           retweetuser_friends_count =  r['retweeted_status']['user']['friends_count']
           retweetuser_created_at =  Helpers.Utils.convertTwitterUTCTimeToLocal(r['retweeted_status']['user']['created_at'])
           retweetuser_lang = r['retweeted_status']['user']['lang']
           retweetuser_following = r['retweeted_status']['user']['following']
           
            
    termo = {'id':post_id,
             'termo':text,
             'criacao':criacao,
             'citacao':citacao,
             'isreply':isreply,
             'replyuserid':replyuserid,
             'replyusername':replyusername,
             'userid':userid,
             'username':username,
             'userlocation':userlocation,
             'followerscount':followerscount,
             'friendscount':friendscount,
             'lang':lang,
             'usercreated':usercreated,
             'following':following,
             'placename':placename,
             'placefullname':placefullname,
             'placecountrycode':placecountrycode,
             'placecountry':placecountry,
             'placetype':placetype,
             'hashtags':hashtags,
             'quotecount':quote_count,
             'replycount':reply_count,
             'retweetcount':reply_count,
             'favoritecount':favorite_count,
             'favorited':favorited,
             'retweeted':retweeted,
             'positivo':positivo,
             'negativo':negativo,
             'sentimento':sentimento,
             'retweet_criacao':retweet_criacao,
             'retweet_id':retweet_id,
             'retweetuser_id':retweetuser_id,
             'retweetuser_location':retweetuser_location,
             'retweetuser_followers_count':retweetuser_followers_count,
             'retweetuser_friends_count':retweetuser_friends_count,
             'retweetuser_created_at':retweetuser_created_at,
             'retweetuser_following':retweetuser_following,
             'entidades':entidades}    
    
    DataAccess.Twitters.db['termos'].insert_one(termo)                

    #Atualiza o controle de termos analizados
    if ('entidades_analyzed' in r):
        aux = r['entidades_analyzed']
    aux.append({'tag':text})
    
    twitters.update_one({'_id':ObjectId(r['_id'])},{"$set":{'entidades_analyzed':aux}},upsert=False)
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    index+=1
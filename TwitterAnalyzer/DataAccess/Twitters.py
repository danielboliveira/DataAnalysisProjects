# -*- coding: utf-8 -*-
"""
Created on Tue May 22 10:21:36 2018

@author: daniel
"""

from pymongo import MongoClient
from bson.objectid import ObjectId

#uri = "mongodb://mongodbtwitters:JwRN8nNQZiJFHxA3UgmNKbOE2MhcryKyueOkWjGEsrnDGucydbkDC62MxjdaZSaNcX8hspHl2QGAEYR9GgLaag==@mongodbtwitters.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"
#client = MongoClient(uri)
client = MongoClient('localhost', 27017)

db = client["TwitterSearch"]
twitters = db['twitters']
wordsSummary = db['wordsSummary']

def updateTwittersUserAnalyzed(listUser):
    for u in listUser:
        twitters.update_many({'user.id':u['id']},{'$set':{'fl_user_processed':True}})
def getUsersForAnalyzer():
    return db['twittersUserIDs'].find()

def getUserInfo(idUser):
    user = db.twitters.find_one({'user.id':idUser},
                            {
                                    '_id':'1',
                                    'user.id':'1', 
                                    'user.name':'1', 
                                    'user.screen_name':'1', 
                                    'user.location':'1', 
                                    'user.protected':'1', 
                                    'user.verified':'1', 
                                    'user.followers_count':'1', 
                                    'user.friends_count':'1', 
                                    'user.listed_count':'1', 
                                    'user.favourites_count':'1', 
                                    'user.statuses_count':'1', 
                                    'user.created_at':'1', 
                                    'user.utc_offset':'1', 
                                    'user.time_zone':'1', 
                                    'user.lang':'1', 
                                    })
    return user

def removeTermo(termo):
    db['termos'].delete_many({'$text':{'$search':termo}})

def removeTermoAnalyzed(termo):
    db.twitters.update_many(
            {'$and': [
                        {'$text':{'$search': termo }} ,
                        {'entidades_analyzed': {'$elemMatch': {'tag': termo}}}
                    ]
            },
            {'$pull': { 'entidades_analyzed': { 'tag': termo } } }
    )

def getTwittersByTermoForAnalyzer(termo):
    consulta = db.twitters.find(
            {'$and': [
                        {'$text':{'$search': termo }} ,
                        {'entidades_analyzed': {'$not': {'$elemMatch': {'tag': termo}}}}
                    ]
            }
    )
#    consulta = db.twitters.aggregate([
#            {'$match': { '$text': { '$search': termo } } },
#            {'$match': {'entidades_geo_analyzed': {'$not': {'$elemMatch': {'tag': termo}}}}}
#    ])
    
    return consulta

def getTotalCitacoesMes():
    consulta =  db['Entidades'].aggregate([
    	{'$project' : { 
              'month' : {'$month' : "$citacao"}, 
              'year' : {'$year' :  "$citacao"},
    		  'total' : 1
          }
    	},
    	{
    	     '$group' : { 
                    '_id' : {'month' : "$month" ,'year' : "$year"},  
                  'total' : {'$sum' : "$total"} 
            }
    	},
    	{
    	  '$project':{
    	    'total':"$total"
    	  }
    	}])
    
    ret = []
    
    for row in consulta:
        id = row['_id']['month'] + row['_id']['year']*12
        t = (id,int(row['_id']['month']),int(row['_id']['year']),int(row['total']))
        ret.append(t)
    
    return sorted(ret)
    
def getTotalCitacoesMesDia():
    consulta =  db['Entidades'].aggregate([
    	{'$project' : { 
              'day':{'$dayOfMonth':'$citacao'},  
              'month' : {'$month' : "$citacao"}, 
              'year' : {'$year' :  "$citacao"},
    		  'total' : 1
          }
    	},
    	{
    	     '$group' : { 
                    '_id' : {'day':'$day','month' : "$month" ,'year' : "$year"},  
                  'total' : {'$sum' : "$total"} 
            }
    	},
    	{
    	  '$project':{
    	    'total':"$total"
    	  }
    	}])
    
    ret = []
    
    for row in consulta:
        id = row['_id']['day'] + row['_id']['month']*30 + row['_id']['year']*365
        t = (id,int(row['_id']['day']),int(row['_id']['month']),int(row['_id']['year']),int(row['total']))
        ret.append(t)
    
    return sorted(ret)


def getTotalCitacoesOrganicaMesDia():
    consulta =  db['EntidadesOrganico'].aggregate([
    	{'$project' : { 
              'day':{'$dayOfMonth':'$citacao'},  
              'month' : {'$month' : "$citacao"}, 
              'year' : {'$year' :  "$citacao"},
              'sentimento':'$sentimento',
    		  'total' : 1
          }
    	},
    	{
    	     '$group' : { 
                    '_id' : {'day':'$day','month' : "$month" ,'year' : "$year",'sentimento':'$sentimento'},  
                  'total' : {'$sum' : "$total"} 
            }
    	},
    	{
    	  '$project':{
    	    'total':"$total"
    	  }
    	}])
    
    ret = []
    
    for row in consulta:
        id = row['_id']['day'] + row['_id']['month']*30 + row['_id']['year']*365
        t = (id,int(row['_id']['day']),int(row['_id']['month']),int(row['_id']['year']),row['_id']['sentimento'],int(row['total']))
        ret.append(t)
    
    return sorted(ret)
    
def getTotalCitacao(termo):
    
    consulta = db['Entidades'].aggregate([
        {'$match': { '$text': { '$search': termo } } },
    	{'$project' : { 
    	      'day':{'$dayOfMonth':'$citacao'},
              'month' : {'$month' : '$citacao'}, 
              'year' : {'$year' :  '$citacao'},
    		  'text' : 1,
    		  'total': 2
          }
    	},
    	{
    	     '$group' : { 
                    '_id' : {'day':'$day','month' : '$month' ,'year' : '$year'},  
                  'total' : {'$sum' : '$total'} 
            }
    	},
    	{
    	  '$project':{
    	    'total':'$total'
    	  }
    	}
            ])

    ret=[]
    for row in consulta:
        id = row['_id']['day'] + row['_id']['month']*30 + row['_id']['year']*365
        dia = int(row['_id']['day'])
        mes = int(row['_id']['month'])
        ano = int(row['_id']['year'])
        total = int(row['total'])
        data = str(dia)+'/'+str(mes)+'/'+str(ano)
        t = (id,data,dia,mes,ano,total,0.0)
        ret.append(t)
        
    return sorted(ret)


def getTotalCitacaoOrganica(termo):
    
    consulta = db['EntidadesOrganico'].aggregate([
        {'$match': { '$text': { '$search': termo } } },
    	{'$project' : { 
    	      'day':{'$dayOfMonth':'$citacao'},
              'month' : {'$month' : '$citacao'}, 
              'year' : {'$year' :  '$citacao'},
              'sentimento':'$sentimento',
    		  'text' : 1,
    		  'total': 2
          }
    	},
    	{
    	     '$group' : { 
                    '_id' : {'day':'$day','month' : '$month' ,'year' : '$year','sentimento':'$sentimento'},  
                  'total' : {'$sum' : '$total'} 
            }
    	},
    	{
    	  '$project':{
    	    'total':'$total'
    	  }
    	}
            ])

    ret=[]
    for row in consulta:
        id = row['_id']['day'] + row['_id']['month']*30 + row['_id']['year']*365
        dia = int(row['_id']['day'])
        mes = int(row['_id']['month'])
        ano = int(row['_id']['year'])
        sentimento = row['_id']['sentimento']
        total = int(row['total'])
        data = str(dia)+'/'+str(mes)+'/'+str(ano)
        t = (id,data,dia,mes,ano,sentimento,total,0.0)
        ret.append(t)
        
    return sorted(ret)
    
    
    
def getTwittersSemSentimento():
    return twitters.find({'sentimento':{'$exists':False}})

def updatePostSentimento(post,positivo,negativo,sentimento):
    twitters.update_one({'_id':ObjectId(post['_id'])},{"$set":{'sentimento':sentimento,'positivo':positivo,'negativo':negativo}},upsert=False)
    
def updatePostDataCriacao(post,data):
    twitters.update_one({'_id':ObjectId(post['_id'])},{"$set":{'criacao':data}},upsert=False)
    
def updatePostWordProcessed(post):
    twitters.update_one({'_id':ObjectId(post['_id'])},{"$set":{'fl_word_processed':1}},upsert=False)

def getTwitterForWordProcess():
    tws = twitters.find(
        {'$or':[
                {'fl_word_processed':{'$exists':False}},
                {'fl_word_processed':{'$eq':0}}
               ]
         },
		 no_cursor_timeout=True
    )
    
    return tws

def getWordSummary(text1,text2):
    return wordsSummary.find_one({"text1":text1,"text2":text2})

def updateWordSummary(text1,text2,word):
    wordsSummary.replace_one({"text1":text1,"text2":text2},word)
    
def insertWordSummary(word):
    wordsSummary.insert_one(word)    
    
    
def getRandomTextTwitters(max_records):
    return db['twittersTrainningData'].find().limit(max_records)

def insertMany(collection):
    twitters.insert_many(collection)

def insertPost(post):
    twitters.insert(post)
    

    
# -*- coding: utf-8 -*-

import DataAccess.Twitters
import Helpers.Utils
import datetime
from bson.objectid import ObjectId

consulta = DataAccess.Twitters.twitters.find(
        {'$and':[
             {'$or':[
                    {'fl_subjects_organico_analyzed':{'$exists':False}},
                    {'fl_subjects_organico_analyzed':{'$eq':False}}
                   ]
             },
             {
                'retweeted_status':{'$exists':False}
             }
         ]
        },
		 no_cursor_timeout=True
)

entsDB = DataAccess.Twitters.db['EntidadesOrganico']

total = consulta.count()
index = 0

if (total > 0):
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

for post in consulta:
    if not ('entidades' in post):
        continue
    
    criacao = datetime.datetime.now()
    if ('criacao' in post):
        criacao = post['criacao']
        
    #Apenas até a hora do post(menor granularidade)    
    citacao = datetime.datetime(criacao.year,criacao.month,criacao.day,criacao.hour,0)    
        
    ents = post['entidades']
    
    sentimento = 'NEUTRO'
    if ('sentimento' in post):
        sentimento = post['sentimento']
    
    for ent in ents:
        dbEnt = entsDB.find_one({'text':ent.upper(),'citacao':citacao,'sentimento':sentimento})
        
        if (dbEnt):
            dbEnt['total'] = dbEnt['total'] + 1
            entsDB.replace_one({'_id':ObjectId(dbEnt['_id'])},dbEnt)
        else:
            dbEnt={'text':ent.upper(),'citacao':citacao,'sentimento':sentimento,'total':1}
            entsDB.insert_one(dbEnt)

    DataAccess.Twitters.twitters.update_one({'_id':ObjectId(post['_id'])},{"$set":{'fl_subjects_organico_analyzed':True}},upsert=False)
    
    Helpers.Utils.printProgressBar(index+1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    index+=1
import DataAccess.Twitters as dao
import Helpers.Utils
import datetime
from dateutil import relativedelta

consulta = dao.getUsersForAnalyzer()
total = consulta.count()
index = 0
lst = []
ilst = 0
Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

agora = datetime.datetime.now()

for user in consulta:
    
    user_id = user['_id']['id']
    user_created = Helpers.Utils.convertTwitterUTCTimeToLocal(user['user_created'])
    
    r = relativedelta.relativedelta(agora, user_created)
    user_age = r.years*12+r.months
    
    first_twitter      = Helpers.Utils.convertTwitterUTCTimeToLocal(user['first_twitter'])
    r = relativedelta.relativedelta(first_twitter, user_created)
    time_first_twitter = r.years*12+r.months
    
    total_twitters = user['total_twitters']
    
    userInfo = dao.getUserInfo(user_id)
    
    user_name = userInfo['user']['name']
    user_screen_name = userInfo['user']['screen_name']
    user_location = userInfo['user']['location']
    user_location_cidade = None
    user_location_pais = None
    
    if (user_location != None):
        aux_location= user_location.split(',')
  
        if (len(aux_location) == 2):
            user_location_cidade = aux_location[0]
            user_location_pais = aux_location[1]
    
    user_protected = userInfo['user']['protected']
    user_verified = userInfo['user']['verified']
    user_followers_count = userInfo['user']['followers_count']
    user_friends_count = userInfo['user']['friends_count']
    user_listed_count = userInfo['user']['listed_count']
    user_favourites_count = userInfo['user']['favourites_count']
    user_statuses_count = userInfo['user']['statuses_count']
    
    if (user_age > 0):
        user_statuses_avg = user_statuses_count / user_age
    else:
        user_statuses_avg = 0
        
    user_lang = userInfo['user']['lang']   
    
    u = dao.db['users'].find_one({'id':user_id})
    
    if (u):
        u['id'] = user_id
        u['user_created'] = user_created
        u['user_age'] = user_age
        u['time_first_twitter'] = time_first_twitter
        u['first_twitter'] = first_twitter
        u['total_twitters'] = total_twitters
        u['user_screen_name'] = user_screen_name
        u['user_location'] = user_location
        u['user_location_cidade'] = user_location_cidade
        u['user_location_pais'] = user_location_pais
        u['user_protected'] = user_protected
        u['user_verified'] = user_verified
        u['user_followers_count'] = user_followers_count
        u['user_friends_count'] = user_friends_count
        u['user_listed_count'] = user_listed_count
        u['user_favourites_count'] = user_favourites_count
        u['user_statuses_count'] = user_statuses_count
        u['user_statuses_avg'] = user_statuses_avg
        u['user_lang'] = user_lang
        dao.db['users'].replace_one({'id':user_id},u)
#        print('Atualização....')
    else:
        u = {'id':user_id,
        'user_created':user_created,
        'user_age':user_age,
        'time_first_twitter':time_first_twitter,
        'first_twitter': first_twitter,
        'total_twitters': total_twitters,
        'user_screen_name': user_screen_name,
        'user_location': user_location,
        'user_location_cidade': user_location_cidade,
        'user_location_pais': user_location_pais,
        'user_protected': user_protected,
        'user_verified': user_verified,
        'user_followers_count': user_followers_count,
        'user_friends_count': user_friends_count,
        'user_listed_count': user_listed_count,
        'user_favourites_count': user_favourites_count,
        'user_statuses_count': user_statuses_count,
        'user_statuses_avg':user_statuses_avg,
        'user_lang': user_lang}
        
        lst.append(u)
        ilst += 1
        
        if (ilst >= 1000):
            dao.updateTwittersUserAnalyzed(lst)
            dao.db['users'].insert_many(lst)
            lst.clear()
            ilst = 0
        
    Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
    index += 1
    
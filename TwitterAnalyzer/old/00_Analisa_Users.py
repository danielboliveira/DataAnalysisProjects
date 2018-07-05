import DataAccess.Twitters
import Helpers.Stats as stats



age_start = 0
age_end = 1

consulta = DataAccess.Twitters.getUsersByAge(age_start,age_end)

posts = list()
ids = list()

for row in consulta:
    posts.append(row['user_statuses_count'])
    ids.append(row['_id'])


media = stats.mean(posts)
mediana = stats.median(posts)
quantil10 = stats.quantile(posts,0.10)
quantil25 = stats.quantile(posts,0.25)
quantil75 = stats.quantile(posts,0.75)
quantil99 = stats.quantile(posts,0.99)
moda = stats.mode(posts)
dispersao = stats.data_range(posts)
desvio_padrao = stats.standard_deviation(posts)
diff_quantile_75_95 = stats.interquartile_range(posts)

print('Idade em meses de {0} a {1}'.format(age_start,age_end))
print('Total de usuários:',len(posts))
print('Medições de interações em posts')
print('media:',media)
print('mediana:',mediana)
print('Quantil 10% menos de :',quantil10,' interações')
print('Quantil 25% menos de :',quantil25,' interações')
print('Quantil 75% menos de :',quantil75,' interações')
print('Quantil 99% menos de :',quantil99,' interações')
print('Quantil Diff 75% e 95%:',diff_quantile_75_95)
print('Moda:',moda)
print('Dispersão:',dispersao)
print('Desvio padrão:',desvio_padrao)

print('\nO 10% mais:')

xy = zip(posts,ids)

for x,y in sorted(xy):
    if (x >= quantil99):
        print('{0} - {1}'.format(y,x))

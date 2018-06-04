import DataAccess.Twitters
import Helpers.Stats as stats



age = 1
consulta = DataAccess.Twitters.getUsersByAge(age)

x = [row['user_statuses_count'] for row in consulta]

media = stats.mean(x)
mediana = stats.median(x)
quantil10 = stats.quantile(x,0.10)
quantil25 = stats.quantile(x,0.25)
quantil75 = stats.quantile(x,0.75)
quantil90 = stats.quantile(x,0.90)
moda = stats.mode(x)
dispersao = stats.data_range(x)
desvio_padrao = stats.standard_deviation(x)
diff_quantile_75_95 = stats.interquartile_range(x)

print('Idade em meses',age)
print('Total de usuários:',len(x))
print('Medições de interações em posts')
print('media:',media)
print('mediana:',mediana)
print('Quantil 10% menos de :',quantil10,' interações')
print('Quantil 25% menos de :',quantil25,' interações')
print('Quantil 75% menos de :',quantil75,' interações')
print('Quantil 90% menos de :',quantil90,' interações')
print('Quantil Diff 75% e 95%:',diff_quantile_75_95)
print('Moda:',moda)
print('Dispersão:',dispersao)
print('Desvio padrão:',desvio_padrao)
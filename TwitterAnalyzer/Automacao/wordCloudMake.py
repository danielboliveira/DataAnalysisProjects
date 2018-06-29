# -*- coding: utf-8 -*- 
import SqlServer.Analysis
from datetime import datetime
from wordcloud import WordCloud
import matplotlib.pyplot as plt

consulta_id = 3
inicio = datetime.strptime('2018-06-27 17:00:00', '%Y-%m-%d %H:%M:%S')
fim = datetime.strptime('2018-06-28 01:30:00', '%Y-%m-%d %H:%M:%S')

print (consulta_id)
print(inicio)
print(fim)
text = SqlServer.Analysis.getWords(consulta_id,inicio,fim)

# Generate a word cloud image
wordcloud = WordCloud(background_color="white").generate(text)

# Display the generated image:
# the matplotlib way:

plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()

# lower max_font_size
#wordcloud = WordCloud(max_font_size=40).generate(text)
#plt.figure()
#plt.imshow(wordcloud, interpolation="bilinear")
#plt.axis("off")
#plt.show()

# The pil way (if you don't have matplotlib)
# image = wordcloud.to_image()
# image.show()


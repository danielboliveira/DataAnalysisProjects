# -*- coding: utf-8 -*- 
import SqlServer.Analysis
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import Helpers.Utils as utils
import traceback
import logging

#Parãmetro para a função
def makeWordsCloud(consulta_id):

    try:
        path = utils.getAnalisesPath(consulta_id)
        #Limpa possíveis arquivos
        fileWordsFull = path+"\\"+"words_full.png"
        utils.removeFile(fileWordsFull)
    
        fileWordsNeg = path+"\\"+"words_negativo.png"
        utils.removeFile(fileWordsNeg)
        
        fileWordsPos = path+"\\"+"words_positivo.png"
        utils.removeFile(fileWordsPos)
        
        #Nuvem com todos os termos
        text = SqlServer.Analysis.getWords(consulta_id)
        wordcloud = WordCloud(background_color="white").generate(text)
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.savefig(fileWordsFull,dpi=400,bbox_inches='tight')
        
        #Nuvem apenas com os temos positivos
        text = SqlServer.Analysis.getWords(consulta_id,'POSITIVO')
        wordcloud = WordCloud(background_color="white").generate(text)
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.savefig(fileWordsPos,dpi=400,bbox_inches='tight')
        
        #Nuvem apenas com os termos negativos
        text = SqlServer.Analysis.getWords(consulta_id,'NEGATIVO')
        wordcloud = WordCloud(background_color="white").generate(text)
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.savefig(fileWordsNeg,dpi=400,bbox_inches='tight')
        
    except Exception as e:
        logging.error(traceback.format_exc())

    


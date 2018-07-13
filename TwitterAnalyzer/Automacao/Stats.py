# -*- coding: utf-8 -*-
import Helpers.Utils as utils
import traceback
import logging
from SqlServer import Analysis as an
import matplotlib.pyplot as mplt
import matplotlib.dates as mdates
from matplotlib.dates import MONDAY
import datetime
import bs4
import os
import pandas as pd

def generateAllOutPuts(id):
    generateStatsSentimentoLineGraphs(id,False,'D',True)
    generateStatsSentimentoLineGraphs(id,True,'D',True)
    generateStatsSentimentoBarGraphs(id,False,'D',True)
    generateStatsSentimentoBarGraphs(id,True,'D',True)
    generateStatsSentimentoPieGraph(id)

def generateProcessConsultaOutput(consulta_id,process_all=False):
    if not process_all:
        path = utils.getAnalisesPath(consulta_id)
        genereateHtmlOutput(path,consulta_id)
        return
    
    termo = an.getTermo(consulta_id)
    if (not termo):
        return
    
    path = utils.__Analises_Root_Path__ + termo
    if not os.path.exists(path):
        return
    
    for _, dirs,_  in os.walk(path):
        for dir in dirs:
            genereateHtmlOutput(path+'\\'+dir,consulta_id)
            
def genereateHtmlOutput(path,consulta_id):
 try:
    termo = an.getTermo(consulta_id)
    if (not termo):
        return
#    
#    path = utils.getAnalisesPath(consulta_id)
#    path = 'C:\\Analise\\lula\\2018_07_12'
    
    with open("./Templates/stats_results.html") as inf:    
       txt = inf.read()
       soup = bs4.BeautifulSoup(txt,'html.parser')

    now =datetime.datetime.now()
    
       
    soup.title.string = soup.title.string + now.strftime('%d/%m/%Y %H:%M')
    
    titulo = soup.select("#titulo")
    if titulo :
        titulo[0].string = 'Termo:{0}'.format(termo.upper())

    footer = soup.find('div',{'id':'footer'})
    if (footer):
       txt = '<b>Resultados gerados em {0}</b>'.format(now.strftime('%d/%m/%Y %H:%M:%S'))
       footer.append(bs4.BeautifulSoup(txt, 'html.parser'))        
        
                         
    graphs = soup.find_all('img')
    for g in graphs:
        if (g['id'] == 'graficoSentimentoLine_img'):
            g['src'] = 'sentimentos_line.png'

        if (g['id'] == 'graficoSentimentoBar_img'):
            g['src'] = 'sentimentos_bar.png'

        if (g['id'] == 'graficoSentimentoLineInfluenciadores_img'):
            g['src'] = 'sentimentos_influencia_line.png'

        if (g['id'] == 'graficoSentimentoBarInfluenciadores_img'):
            g['src'] = 'sentimentos_influencia_bar.png'

    
    divs =  soup.find_all('div')
    for d in divs:

        #table de dados de origem
        if (d.has_attr('id') and d['id'] == 'table_graficoSentimento'):
            if (os.path.exists(path+'\dados.csv')):
                df = pd.read_csv(path+'\dados.csv',sep='\t')
                df['%Negativos'] = df['%Negativos']*100
                df['%Positivos'] = df['%Positivos']*100
                df['%Neutros'] = df['%Neutros']*100
                df.rename(columns={'Unnamed: 0':'Data'},inplace=True)
                t = df.to_html(float_format=lambda x: '%10.2f' % x)
                if (d):
                    d.append(bs4.BeautifulSoup(t,'html.parser'))
                    d.append(bs4.BeautifulSoup('<p>Totalizadores</p>','html.parser'))
                    d.append( bs4.BeautifulSoup(df.describe().to_html(float_format=lambda x: '%10.2f' % x),'html.parser') )
        
        if (d.has_attr('id') and d['id'] == 'table_graficoSentimentoInfluenciadores'):
            if (os.path.exists(path+'\dados_influencia.csv')):
                df = pd.read_csv(path+'\dados_influencia.csv',sep='\t')
                df['%Negativos'] = df['%Negativos']*100
                df['%Positivos'] = df['%Positivos']*100
                df['%Neutros'] = df['%Neutros']*100
                df.rename(columns={'Unnamed: 0':'Data'},inplace=True)
                t = df.to_html(float_format=lambda x: '%10.2f' % x)
                if (d):
                    d.append(bs4.BeautifulSoup(t,'html.parser'))
                    d.append(bs4.BeautifulSoup('<p>Totalizadores</p>','html.parser'))
                    d.append(bs4.BeautifulSoup(df.describe().to_html(float_format=lambda x: '%10.2f' % x),'html.parser') )

    with open(path+'\\results.html', "w") as outf:
      outf.write(str(soup))
      
 except Exception as e:
    logging.error(traceback.format_exc())
    
#plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')    
#fig = plt.get_figure()
#fig.savefig("output.png")        
#plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento')        

def generateStatsSentimentoLineGraphs(consulta_id,somente_influenciadores=False,resample='D',export=False): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores=somente_influenciadores)
        
        
        xs = df.index.values
        ypos = df['%Positivos'].values
        yneg = df['%Negativos'].values
        yneu = df['%Neutros'].values
        
        if (resample.lower() == 'm'): #xticks Mês
            major_locator = mdates.MonthLocator(range(1, 13))
            minor_locator = mdates.WeekdayLocator(MONDAY) 
            major_formatter = mdates.DateFormatter('%b %y')
            xlabel = 'Mes(es)'
        else:
            major_locator = mdates.DayLocator() 
            minor_locator = mdates.DayLocator()
            major_formatter = mdates.DateFormatter('%d %b')
            xlabel = 'Dia(s)'
        
        mplt.clf()    
        fig, ax = mplt.subplots()
        ax.plot(xs,ypos,'b',label='% Positivos')
        ax.plot(xs,yneg,'r',label='% Negativos')
        ax.plot(xs,yneu,'gray',label='% Neutros')
        
        ax.xaxis.set_major_locator(major_locator)
        ax.xaxis.set_major_formatter(major_formatter)
        ax.xaxis.set_minor_locator(minor_locator)
       
        ax.set_ylim(0,1)
        ax.legend(loc='best', shadow=True, fontsize='small')
        ax.autoscale_view()
        
        fig.set_size_inches(10,4)
        fig.autofmt_xdate()

        
        mplt.xlabel(xlabel)
        mplt.ylabel('Percentuais')
        
        if (not somente_influenciadores):
            mplt.title('Análise sentimental - Termo:{0}'.format(termo))
        else:
            mplt.title('Análise sentimental - Termo:{0} (Apenas influenciadores)'.format(termo))

        if (not export):             
            mplt.show()  
            print(df)
            print()
            print(df.describe())
        else:
            path = utils.getAnalisesPath(consulta_id) 
            mask = '%Y_%m_%d'
            file_prefix =  datetime.datetime.now().strftime(mask)
            
            if not somente_influenciadores:
                file = path+"\\"+"sentimentos_line.png"
                file_csv = path+"\\"+"dados.csv"
                file_describe_csv = path+"\\"+"dados_describe.csv"
            else:
                file = path+"\\"+"sentimentos_influencia_line.png"
                file_csv = path+"\\"+"dados_influencia.csv"
                file_describe_csv = path+"\\"+"dados_describe_influencia.csv"
            
            utils.removeFile(file)
            fig.savefig(file)
            df.to_csv(file_csv,sep='\t')
            df.describe().to_csv(file_describe_csv,sep='\t')
        
    except Exception as e:
        logging.error(traceback.format_exc())
        

def generateStatsSentimentoBarGraphs(consulta_id,somente_influenciadores=False,resample='D',export=False): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores=somente_influenciadores)
        
        
        xs = df.index.values
        ypos = df['%Positivos'].values
        yneg = df['%Negativos'].values
        yneu = df['%Neutros'].values

        fig, ax = mplt.subplots()
        fig.set_size_inches(10,4)
        
        ppos = mplt.bar(xs, ypos,color='b')
        pneu = mplt.bar(xs, yneu,bottom=ypos,color='gray')
        pneg = mplt.bar(xs, yneg,bottom=ypos+yneu,color='r')
        
        mplt.ylim([0,1])
        mplt.yticks(fontsize=12)
        mplt.ylabel('Percentuais', fontsize=12)
        mplt.legend((ppos[0],pneu[0],pneg[0]), ('Positivos', 'Negativos','Neutros'))
        
        if (resample.lower() == 'm'): #xticks Mês
            major_locator = mdates.MonthLocator(range(1, 13))
            minor_locator = mdates.WeekdayLocator(MONDAY) 
            major_formatter = mdates.DateFormatter('%b %y')
            xlabel = 'Mes(es)'
        else:
            major_locator = mdates.DayLocator() 
            minor_locator = mdates.DayLocator()
            major_formatter = mdates.DateFormatter('%d %b')
            xlabel = 'Dia(s)'
        
        mplt.xlabel(xlabel, fontsize=12)
        ax.xaxis.set_major_locator(major_locator)
        ax.xaxis.set_major_formatter(major_formatter)
        ax.xaxis.set_minor_locator(minor_locator)
        ax.set_ylim(0,1)
        ax.autoscale_view()
        
        if (not somente_influenciadores):
            mplt.title('Análise sentimental - Termo:{0}'.format(termo))
        else:
            mplt.title('Análise sentimental - Termo:{0} (Apenas influenciadores)'.format(termo))

        if (not export):             
            mplt.show()  
        else:
            path = utils.getAnalisesPath(consulta_id)
            mask = '%Y_%m_%d'
            file_prefix =  datetime.datetime.now().strftime(mask)
            
            if not somente_influenciadores:
                file = path+"\\"+"sentimentos_bar.png"
            else:
                file = path+"\\"+"sentimentos_influencia_bar.png".format(file_prefix)

            utils.removeFile(file)
            fig.savefig(file)
        
    except Exception as e:
        logging.error(traceback.format_exc())

def generateStatsSentimentoGraphs(consulta_id,time_series=False,resample='H',xticks = 'd'): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        if (time_series == False):
            df = an.getStatsTwitters(consulta_id)
        else:
            df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores = False)
        
        path = utils.getAnalisesPath(consulta_id)
        mask = '%Y_%m_%d'
        file_prefix =  datetime.datetime.now().strftime(mask)

        file_graph_1 = path+"\\"+"sentimentos_line.png"
        utils.removeFile(file_graph_1)
        
        if not time_series:
            plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))        
        else:
            plt = df.plot(figsize=(20,5),kind='line',x=df.index,y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))        
           
        
        fig = plt.get_figure()
        fig.savefig(file_graph_1)
        
        file_graph_2 = path+"\\"+"sentimentos_bar.png"
        utils.removeFile(file_graph_2)
        
        if not time_series:
            plt = df.plot(figsize=(10,6),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))    
        else:
            plt = df.plot(figsize=(10,6),kind='bar',x=df.index,rot=45,y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0})'.format(termo.upper()))    
            
            


        fig = plt.get_figure()
        fig.savefig(file_graph_2)

        #Somentes os posts de influência
        if (time_series == False):
            df = an.getStatsTwitters(consulta_id,True)
        else:
            df = an.getStatsTwittersTimeSeries(consulta_id,resample = resample,somente_influenciadores = True)
        
        
        file_graph_1 = path+"\\"+"sentimentos_line_influencia.png"
        utils.removeFile(file_graph_1)
        
        if not time_series:
            plt = df.plot(figsize=(20,5),kind='line',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))        
        else:
            plt = df.plot(figsize=(20,5),kind='line',x=df.index,y=['%Positivos','%Neutros','%Negativos'],color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))        
            
        fig = plt.get_figure()
        fig.savefig(file_graph_1)        
        
        file_graph_2 = path+"\\"+"{0}_sentimentos_bar_influencia.png".format(file_prefix)
        utils.removeFile(file_graph_2)
        
        if not time_series:
            plt = df.plot(figsize=(10,4),kind='bar',x=['Horario'],y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))
        else:
            plt = df.plot(figsize=(10,4),kind='bar',x=df.index,y=['%Positivos','%Neutros','%Negativos'],stacked=True,color=['b', 'lightgray', 'r'],title = 'Variação de sentimento({0}) - Apenas Perfis de Influência'.format(termo.upper()))    
        
        fig = plt.get_figure()
        fig.savefig(file_graph_2) 
        
        
    except Exception as e:
        logging.error(traceback.format_exc())
        
    
def generateStatsSentimentoPieGraph(consulta_id): 
    try:    
        termo = an.getTermo(consulta_id)
        
        if (not termo):
            return
        
        df = an.getStatsTwitters(consulta_id)
        
        path = utils.getAnalisesPath(consulta_id)
        
        file_graph_1 = path+"\\"+"sentimentos_pie.png"
        utils.removeFile(file_graph_1)
        
        
        ds = df.describe()
        r = ds.drop(['std','count','min','max','25%','50%','75%']).drop(['Negativos','Neutros','Positivos'],axis=1)
        
        prcNeg = r['%Negativos'][0]
        prcPos = r['%Positivos'][0]
        prcNeu = r['%Neutros'][0]
        
        # Data to plot
        labels = 'Negativos', 'Positivos', 'Neutros'
        sizes = [prcNeg, prcPos, prcNeu]
        colors = ['red', 'blue', 'silver']
#        explode = (0.1, 0, 0, 0)  # explode 1st slice
         
        # Plot
        mplt.clf()
        mplt.pie(sizes, labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=140)
        mplt.title('Análise sentimental - Termo:{0}'.format(termo))
        mplt.axis('equal')
        mplt.savefig(file_graph_1)        
        
    except Exception as e:
        logging.error(traceback.format_exc())

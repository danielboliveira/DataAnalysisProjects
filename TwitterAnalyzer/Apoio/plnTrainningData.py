from pymongo import MongoClient
import csv
import re
import Helpers.Utils

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

    

#nlp = spacy.load('C:/Cloud/Google Drive/Documents/python/TwitterAnalyzer/Model/')
    
client = MongoClient('localhost', 27017)
db = client["TwitterSearch"]
twitters = db['twittersTrainningData']

total = 1000
index = 0

pos=['patriota','apoio','apoia','inocente','adorável',	'afável',	'afetivo',	'agradável',	'ajuizado',	'alegre',	'altruísta',	'amável',	'amigável',	'amoroso',	'aplicado',	'assertivo',	'atencioso',	'atento',	'autêntico',	'aventureiro',	'bacana',	'benévolo',	'bondoso',	'brioso',	'calmo',	'carinhoso',	'carismático',	'caritativo',	'cavalheiro',	'cívico',	'civilizado',	'companheiro',	'compreensivo',	'comunicativo',	'confiante',	'confiável',	'consciencioso',	'corajoso',	'cordial',	'cortês',	'credível',	'criativo',	'criterioso',	'cuidadoso',	'curioso',	'decente',	'decoroso',	'dedicado',	'descontraído',	'desenvolto',	'determinado',	'digno',	'diligente',	'disciplinado',	'disponível',	'divertido',	'doce',	'educado',	'eficiente',	'eloquente',	'empático',	'empenhado',	'empreendedor',	'encantador',	'engraçado',	'entusiasta',	'escrupuloso',	'esforçado',	'esmerado',	'esperançoso',	'esplêndido',	'excelente',	'extraordinário',	'extrovertido',	'é feliz',	'é feliz',	'fiel',	'fofo',	'forte',	'franco',	'generoso',	'gentil',	'genuíno',	'habilidoso',	'honesto',	'honrado',	'honroso',	'humanitário',	'humilde',	'idôneo',	'imparcial',	'independente',	'inovador',	'íntegro',	'inteligente',	'inventivo',	'justo',	'leal',	'legal',	'livre ',	'maduro',	'maravilhoso',	'meigo',	'modesto',	'natural',	'nobre',	'observador',	'organizado',	'otimista',	'ousado',	'pacato',	'paciente',	'perfeccionista',	'perseverante',	'persistente',	'perspicaz',	'ponderado',	'pontual',	'preocupado',	'preparado',	'prestativo',	'prestável',	'proativo',	'produtivo',	'prudente',	'racional',	'respeitador',	'responsável',	'sábio',	'sagaz',	'sensato',	'sensível',	'simpático',	'sincero',	'solícito',	'solidário',	'sossegado',	'ternurento',	'tolerante',	'tranquilo',	'transparente',	'valente',	'valoroso',	'VERDADEIRO',	'zeloso','meu presidente','meu amigo']
neg=['alienar','ilícito','doente','corrupcao','cadeia para','cadeia','rejeita','rejeitar','rejeição','ingovernabilidade','safado','corrupto','lama','podridão','lixo','ladrão',	'covarde',	'coisa de covarde',	'coisa de bom home',	'Acéfalo',	'Alapoado',	'Aldrabão',	'Alorpado',	'Asqueroso',	'Bargante',	'Beldroega',	'Beócio',	'Biltre',	'Boçal',	'Bucéfalo',	'Burlão',	'Calatrão',	'Canalha',	'Capadócio',	'Cariote',	'Celerado',	'Charlatão',	'Cretino',	'Descerebrado',	'Desgraçado',	'Embusteiro',	'Energúmeno',	'Espurco',	'Espúrio',	'Estrupício',	'Esquálido',	'Facínora',	'Fariseu',	'Filisteu',	'Haloplanta',	'Histrião',	'Ignóbil',	'Imbecil',	'Impostor',	'Infausto',	'Infeliz',	'Intrujão',	'Jogral',	'Labroste',	'Labrego',	'Lapão',	'Lapônio',	'Lapúrdio',	'Lazarento',	'Lorpa',	'Macavenco',	'Maloio',	'Mentecapto',	'Mesquinho',	'Mequetrefe',	'Morcão',	'Nefasto',	'Néscio',	'Obtuso',	'Pacóvio',	'Parlapatão',	'Parvo',	'Patife',	'Pérfido',	'Pusilânime',	'Pustulento',	'Repugnante',	'Sendeiro',	'Sórdido',	'Torpe',	'Trapaceiro',	'Trambiqueiro',	'Vacão',	'Vil',	'Ximbéu',	'Xucro',	'Zelote',	'agressivo',	'ansioso',	'antipático',	'antissocial',	'apático',	'apressado',	'arrogante',	'atrevido',	'autoritário',	'avarento',	'birrento',	'bisbilhoteiro',	'bruto',	'calculista',	'casmurro',	'chato',	'cínico',	'ciumento',	'colérico',	'comodista',	'covarde',	'crítico',	'cruel',	'debochado',	'depressivo',	'desafiador',	'desbocado',	'descarado',	'descomedido',	'desconfiado',	'descortês',	'desequilibrado',	'desleal',	'desleixado',	'desmazelado',	'desmotivado',	'desobediente',	'desonesto',	'desordeiro',	'despótico',	'desumano',	'discriminador',	'dissimulado',	'distraído',	'egoísta',	'estourado',	'estressado',	'exigente',	'FALSO',	'fingido',	'fraco',	'frio',	'frívolo',	'fútil',	'ganancioso',	'grosseiro',	'grosso',	'hipócrita',	'ignorante',	'impaciente',	'impertinente',	'impetuoso',	'impiedoso',	'imponderado',	'impostor',	'imprudente',	'impulsivo',	'incompetente',	'inconstante',	'inconveniente',	'incorreto',	'indeciso',	'indecoroso',	'indelicado',	'indiferente',	'infiel',	'inflexível',	'injusto',	'inseguro',	'insensato',	'insincero',	'instável',	'insuportável',	'interesseiro',	'intolerante',	'intransigente',	'introvertido',	'irracional',	'irrascível',	'irrequieto',	'irresponsável',	'irritadiço',	'malandro',	'maldoso',	'malicioso',	'malvado',	'mandão',	'manhoso',	'maquiavélico',	'medroso',	'mentiroso',	'mesquinho',	'narcisista',	'negligente',	'nervoso',	'neurótico',	'obcecado',	'odioso',	'oportunista',	'orgulhoso',	'pedante',	'pessimista',	'possessivo',	'precipitado',	'preconceituoso',	'preguiçoso',	'prepotente',	'presunçoso',	'problemático',	'quezilento',	'rancoroso',	'relapso',	'rigoroso',	'rabugento',	'rude',	'sarcástico',	'sedentário',	'teimoso',	'tímido',	'tirano',	'traiçoeiro',	'traidor',	'trapaceiro',	'tendencioso',	'trocista',	'vagabundo',	'vaidoso',	'vulnerável',	'vigarista',	'xenófobo',	'porra','cú','puto','puta','preso','prisão','sentença']

def contemPositivo(text):
    for w in pos:
        if (text.lower().find(w.lower()) >= 0):
            return 1
    return 0    

def contemNegativo(text):
    for w in neg:
        if (text.lower().find(w.lower()) >= 0):
            return 1
    return 0
        
Helpers.Utils.printProgressBar(index,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)

## WITH ###
with open('trainningRawData.csv', 'w',newline='', encoding='utf-8') as csvfile:
    fieldnames = ['text','POSITIVE','NEGATIVE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for post in twitters.find():
     clean_sentence = Helpers.Utils.get_text_sanitized(post,True)
     clean_sentence = ' '.join(re.sub("(@[A-Za-z0-9]+)|(\w+:\/\/\S+)"," ",str(clean_sentence)).split())
     clean_sentence = re.sub(emoji_pattern, '', clean_sentence)
     
     if not (len(clean_sentence) <= 3):
         pos = neg = 0
         neg = contemNegativo(clean_sentence) #negatividade mais forte
         
         if (neg == 0):
             pos = contemPositivo(clean_sentence)
             
         writer.writerow({'text':clean_sentence,'POSITIVE':pos,'NEGATIVE':neg})

     Helpers.Utils.printProgressBar(index + 1,total,prefix = 'Progress:', suffix = 'Complete',showAndamento=True)
     index += 1
     
         

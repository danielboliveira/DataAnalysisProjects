--exec prcProcessaTermosInteresesSentimento

select upper(a.text) as Termo,
       dt_referencia as Referencia,
	   qt_negativo as Negativo,
	   qt_positivo as Positivo,
	   qt_neutro as Neutro
  from TermoInteresse a 
  join AnaliseSentimento b on a.id = b.id_termo
where a.text = 'copa do mundo' and dt_referencia >= '2018-5-10'
order by termo,dt_referencia desc
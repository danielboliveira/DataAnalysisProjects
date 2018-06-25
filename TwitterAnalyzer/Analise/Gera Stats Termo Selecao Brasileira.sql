declare @termo varchar(100)
declare @inicio datetime
declare @fim datetime
declare @processamento datetime

set @termo = 'seleção brasileira'

set @processamento = getdate()
set @inicio = '2018-6-22 08:00:00'
set @fim = '2018-06-22 12:30:00'

select Replicate('0',2-len(Horas))+Cast(horas as varchar) + ':' + replicate('0',2-len(minutos))+ cast(minutos as varchar) as Horario,
       SUM(qt_positivo) as Positivo,
	   SUM(qt_neutro) as Neutro, 
	   sum(qt_negativo) as Negativo
from(
		select  DatePart(Hour,dt_twitter) as horas,
				case 
				  when DatePart(MINUTE,dt_twitter) > 0   and  DatePart(MINUTE,dt_twitter) <= 10  then 10 
				  when DatePart(MINUTE,dt_twitter) > 10  and  DatePart(MINUTE,dt_twitter) <= 20 then 20
				  when DatePart(MINUTE,dt_twitter) > 20 and  DatePart(MINUTE,dt_twitter) <= 30 then 30
				  when DatePart(MINUTE,dt_twitter) > 30 and  DatePart(MINUTE,dt_twitter) <= 40 then 40
				  when DatePart(MINUTE,dt_twitter) > 40 and  DatePart(MINUTE,dt_twitter) <= 50 then 50
				  when DatePart(MINUTE,dt_twitter) > 50 then 0
				  else 0 end Minutos,
			   sum(CASe when sentimento = 'POSITIVO' then 1 else 0 end) as Qt_Positivo,
			   sum(CASe when sentimento = 'NEUTRO' then 1 else 0 end) as Qt_Neutro,
			   sum(CASe when sentimento = 'NEGATIVO' then 1 else 0 end) as Qt_Negativo
		from
		(
		select CAST(a.created_at as smalldatetime) as dt_twitter, a.sentimento
			from twitter a
			join [user] b on b.id = a.user_id
		where a.created_at >= @inicio and a.created_at <= @fim
			  and a.text like '%seleção brasileira%'
			  and a.sentimento is not null
			  --Pegando Twitters originais
			  and a.retweeted_status_id is null
			  --Apenas Usuários verificados
			  and b.verified = 'FALSE'
		) as T
		group by T.dt_twitter
--		Order by T.dt_twitter
) T
group by horas,minutos
order by horas*60+minutos



select top 10 a.id,
b.screen_name,
a.text,
MAX(a.reply_count) as total
from twitter a join [user] b on a.user_id = b.id
where a.created_at >=  '2018-6-22 08:00:00' and a.created_at <=  '2018-6-22 12:30:00'
 and a.text like '%seleção brasileira%'
 and a.sentimento is not null
 --Pegando Twitters originais
 and a.retweeted_status_id is null
group by a.id,a.text,b.screen_name
order by 4 desc

select a.id,SUM(a.[retweet_count])
  from twitter a
where a.retweeted_status_id = 1010158902795816960
group by a.id
order by 2 desc










/*
select Replicate('0',2-len(Horas))+Cast(horas as varchar) + ':' + replicate('0',2-len(minutos))+ cast(minutos as varchar) as Horario,
       SUM(qt_positivo) as Positivo,
	   SUM(qt_neutro) as Neutro, 
	   sum(qt_negativo) as Negativo
from(
		select  DatePart(Hour,dt_twitter) as horas,
				case 
				  when DatePart(MINUTE,dt_twitter) > 0  and  DatePart(MINUTE,dt_twitter) <= 5  then 5 
				  when DatePart(MINUTE,dt_twitter) > 5  and  DatePart(MINUTE,dt_twitter) <= 10 then 10
				  when DatePart(MINUTE,dt_twitter) > 10 and  DatePart(MINUTE,dt_twitter) <= 15 then 15
				  when DatePart(MINUTE,dt_twitter) > 15 and  DatePart(MINUTE,dt_twitter) <= 20 then 20
				  when DatePart(MINUTE,dt_twitter) > 20 and  DatePart(MINUTE,dt_twitter) <= 25 then 25
				  when DatePart(MINUTE,dt_twitter) > 25 and  DatePart(MINUTE,dt_twitter) <= 30 then 30
				  when DatePart(MINUTE,dt_twitter) > 30 and  DatePart(MINUTE,dt_twitter) <= 35 then 35
				  when DatePart(MINUTE,dt_twitter) > 35 and  DatePart(MINUTE,dt_twitter) <= 40 then 40
				  when DatePart(MINUTE,dt_twitter) > 40 and  DatePart(MINUTE,dt_twitter) <= 45 then 45
				  when DatePart(MINUTE,dt_twitter) > 45 and  DatePart(MINUTE,dt_twitter) <= 50 then 50
				  when DatePart(MINUTE,dt_twitter) > 50 and  DatePart(MINUTE,dt_twitter) <= 55 then 55
				  when DatePart(MINUTE,dt_twitter) > 55 and  DatePart(MINUTE,dt_twitter) <= 59 then 0
				  else 0 end Minutos,
			   sum(CASe when sentimento = 'POSITIVO' then 1 else 0 end) as Qt_Positivo,
			   sum(CASe when sentimento = 'NEUTRO' then 1 else 0 end) as Qt_Neutro,
			   sum(CASe when sentimento = 'NEGATIVO' then 1 else 0 end) as Qt_Negativo
		from
		(
		select CAST(created_at as smalldatetime) as dt_twitter, a.sentimento
			from twitter a
		where a.created_at >= @inicio and a.created_at <= @fim
			  and a.text like '%seleção brasileira%'
			  and a.sentimento is not null
		) as T
		group by T.dt_twitter
--		Order by T.dt_twitter
) T
group by horas,minutos
order by horas*60+minutos
*/

/*
while(@aux <= @fim) begin
	
	if not exists(select 1 from stats_termos where ds_termo = @termo and dt_referencia = @aux) begin
	   insert into stats_termos ([ds_termo],[dt_referencia],[dt_processamento]) values (@termo,@aux,@processamento)
    end

	declare @pos int
	declare @neg int 
	declare @neu int 

	set @pos = (select count(*)
	             from twitter a
				 where a.created_at >= @aux and a.created_at < DATEADD(day,1,@aux)
				   and a.text like '%'+@termo+'%'
				   and a.sentimento = 'POSITIVO')
	
	set @neg = (select count(*)
	             from twitter a
				 where a.created_at >= @aux and a.created_at < DATEADD(day,1,@aux)
				   and a.text like '%'+@termo+'%'
				   and a.sentimento = 'NEGATIVO')
	
	set @neu = (select count(*)
	             from twitter a
				 where a.created_at >= @aux and a.created_at < DATEADD(day,1,@aux)
				   and a.text like '%'+@termo+'%'
				   and a.sentimento = 'NEUTRO')
	
	update stats_termos
	  set qt_positivo = @pos,qt_negativo = @neg, qt_neutro = @neu
	  where ds_termo = @termo and dt_referencia = @aux

	set @aux = dateadd(day,1,@aux)
end

select Convert(varchar(10),DATEADD(DAY, 1-DATEPART(WEEKDAY, dt_referencia),dt_referencia),103) as InicioSemana,
       Convert(varchar(10),DateAdd(Day,7,DATEADD(DAY, 1-DATEPART(WEEKDAY, dt_referencia), dt_referencia)),103) as FimSemana,
       sum(qt_positivo) as qt_positivo, 
	   sum(qt_negativo) as qt_negativo,
	   sum(qt_neutro) as qt_neutro 
from stats_termos 
where dt_referencia >= @inicio and dt_referencia <= @fim and ds_termo = @termo
group by  DATEADD(DAY, 1-DATEPART(WEEKDAY, dt_referencia),dt_referencia),
          DateAdd(Day,7,DATEADD(DAY, 1-DATEPART(WEEKDAY, dt_referencia), dt_referencia))
order by DATEADD(DAY, 1-DATEPART(WEEKDAY, dt_referencia),dt_referencia)
*/
/*
select Convert(varchar(10),b.created_at,103) as Data,
       count(*) as Total
  from entidade a
  join twitter b on a.twitter_id = b.id
where b.created_at >= '2018-5-1' and a.text like '%Seleção Brasileira%' and sentimento = 'Positivo'
group by Convert(varchar(10),b.created_at,103)
order by 1

select Convert(varchar(10),b.created_at,103) as Data,
       count(*) as Total
  from twitter b
where b.created_at >= '2018-5-1' and b.text like '%Copa%' and sentimento = 'Negativo'
group by Convert(varchar(10),b.created_at,103)
order by 1
*/
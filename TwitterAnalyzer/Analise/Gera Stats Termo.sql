declare @termo varchar(100)
declare @inicio date
declare @fim date
declare @processamento datetime

set @termo = 'bolsonaro'

set @processamento = getdate()
set @inicio = '2018-5-15'
set @fim = '2018-06-15'

declare @aux Date
set @aux = @inicio

delete from stats_termos where ds_termo = @termo and dt_referencia >= @inicio and dt_referencia <= @fim

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
create procedure prcGetStatsSentimento
@cd_consulta int,
@inicio datetime,
@fim datetime

as

--set @cd_consulta = 1
--set @inicio = '2018-6-26 00:00:00'
--set @fim = '2018-06-27 14:00:00'

--Limpa estatísitcas anteriores
delete from [dbo].[stats_sentimento_termo]
where dt_stats >= CAST(@inicio as date) 
  and dt_stats <= CAST(@fim as date)
  and cd_consulta = @cd_consulta
  
declare @termo varchar(200)
set @termo = (select ds_termo from [dbo].[twitter_consulta] where cd_consulta = @cd_consulta)

declare @processamento datetime
set @processamento = getdate()

insert into [dbo].[stats_sentimento_termo] ([cd_consulta], 
                                            [dt_processamento], 
											[dt_stats], 
											[hr_stats], 
											[mn_stats], 
											[qt_positivo], 
											[qt_neutro], [qt_negativo])
select @cd_consulta,
       @processamento,
       Data,
       Horas,
       Minutos,
       SUM(qt_positivo) as Positivo,
	   SUM(qt_neutro) as Neutro, 
	   sum(qt_negativo) as Negativo
from(
		select  Cast(dt_twitter as date) as Data,
		        DatePart(Hour,dt_twitter) as horas,
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
			from twitterAnalyzer.dbo.twitter a
		where a.created_at >= @inicio and a.created_at <= @fim
			  and a.text like '%' + @termo + '%'
			  and a.sentimento is not null
		) as T
		group by T.dt_twitter
) T
group by [data],horas,minutos
order by [data],horas*60+minutos
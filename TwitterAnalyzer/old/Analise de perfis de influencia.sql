drop table sumario_stats_twitters
go
declare @termo varchar(100)
declare @inicio datetime
declare @fim datetime
declare @processamento datetime

set @termo = 'seleção brasileira'

set @processamento = getdate()
set @inicio = '2018-6-22 08:00:00'
set @fim = '2018-06-22 12:30:00'

select a.id as PostID,a.created_at,a.sentimento, a.text as Post_Text,b.id as UserID, b.screen_name,
             (select COUNT (*) from twitter tw where tw.in_reply_to_status_id = a.id 
			     and tw.created_at >=  @inicio and tw.created_at <=  @fim) as TotalRespostas,
			 (select COUNT(*) from twitter tw where tw.quoted_status_id = a.id
			     and tw.created_at >=  @inicio and tw.created_at <=  @fim) as TotalCitacoes,
			 (select COUNT(*) from twitter tw where tw.retweeted_status_id = a.id
			   and tw.created_at >=  @inicio and tw.created_at <=  @fim) as TotalRetweeted
into sumario_stats_twitters
from twitter a join [user] b on a.user_id = b.id
where a.created_at >=  @inicio and a.created_at <=  @fim
 and a.text like '%'+@termo+'%'
 and a.sentimento is not null
 --Pegando Twitters originais
 and a.retweeted_status_id is null
 and b.verified = 'FALSE'
 go
 select top 5 * 
 from sumario_stats_twitters
 order by TotalRetweeted desc

 go

 select top 5 * 
 from sumario_stats_twitters
 where sumario_stats_twitters.Post_Text like '%neymar%' and sentimento = 'NEGATIVO'
 order by TotalRetweeted desc

 go

 select Replicate('0',2-len(Horas))+Cast(horas as varchar) + ':' + replicate('0',2-len(minutos))+ cast(minutos as varchar) as Horario,
       sum(case when sentimento = 'POSITIVO' then TotalRetweeted else 0 end) as qt_positivos,
	   sum(case when sentimento = 'NEUTRO' then TotalRetweeted else 0 end) as qt_neutros,
	   sum(case when sentimento = 'NEGATIVO' then TotalRetweeted else 0 end) as qt_negtivos
 from
 (
	 select DatePart(hour,created_at) as horas,
			dbo.fncParticiona5Minutos(created_at) as minutos,
			sentimento,
			TotalRetweeted
	 from sumario_stats_twitters
 )T
 group by horas,minutos
 order by horas*60 + minutos

 select DelayCompartilhamento ,COUNT(*) total
 from(
 select DATEDIFF(MINUTE,(select b.created_at from twitter b where b.id = a.retweeted_status_id),a.created_at) as DelayCompartilhamento,
        a.id
  from twitter a
where exists(select 1 from sumario_stats_twitters ss where ss.PostID = a.retweeted_status_id and ss.sentimento = 'NEGATIVO')
) T
group by T.DelayCompartilhamento
order by 1



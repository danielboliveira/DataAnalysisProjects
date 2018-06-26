drop table sumario_stats_twitters
go
declare @termo varchar(100)
declare @inicio datetime
declare @fim datetime
declare @processamento datetime

set @termo = 'seleção brasileira'

set @processamento = getdate()
set @inicio = '2018-6-22 08:00:00'
set @fim = '2018-06-22 12:00:00'

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

 select horario,SUM(totalRetweeted) as Total
 from
 (
 select  DatePart(hour,created_at) as hora,
         fncParticiona5Minutos(created_at) as minutos,
               TotalRetweeted
 from sumario_stats_twitters
 where sentimento = 'Negativo'
 )T
 group by horario


 order by DATEPART(hour,created_at)*60 + DATEPART(MINUTE,created_at)









 select * from [user] where id = 859892492560453632

 select DelayCompartilhamento,COUNT(*) total
 from(
 select DATEDIFF(MINUTE,(select b.created_at from twitter b where b.id = a.retweeted_status_id),a.created_at) as DelayCompartilhamento,
        a.id
  from twitter a join [user] b on b.id = a.user_id
where a.retweeted_status_id = 1010161535212314626
) T
group by T.DelayCompartilhamento


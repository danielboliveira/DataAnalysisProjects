create procedure prcGetStatsRetweetsDelay
@consulta_id int,
@inicio datetime,
@fim datetime
as
/*
set @consulta_id = 1
set @inicio = '2018-6-26 00:00:00'
set @fim = '2018-06-27 14:00:00'
*/

declare @termo varchar(100)
set @termo = (select ds_termo from [dbo].[twitter_consulta] where cd_consulta = @consulta_id)

declare @processamento datetime
set @processamento = getdate()

delete from stats_rts
where cd_consulta = @consulta_id
   and dt_post >= @inicio and dt_post <= @fim

--Tempo para realizar o retwitte
insert into stats_rts
	select @consulta_id,
	       @processamento,
		   cast(a.created_at as date),
		   rt.sentimento,
		   DATEDIFF(MINUTE,rt.created_at,a.created_at) as Delay,COUNT(a.id) as total
	from twitterAnalyzer.dbo.twitter a
	  join twitterAnalyzer.dbo.twitter rt on rt.id = a.retweeted_status_id
	where a.created_at >=  @inicio and a.created_at <=  @fim
	 and a.text like '%'+@termo+'%'
	 and a.sentimento is not null
	 and a.consulta_id = @consulta_id
	group by cast(a.created_at as date),
		     rt.sentimento,
		     DATEDIFF(MINUTE,rt.created_at,a.created_at)
    order by 1

		/*
create table stats_rts
(
  cd_stats_rts bigint identity(1,1) not null primary key,
  cd_consulta int not null,
  dt_stats datetime not null,
  dt_post  date not null,
  ds_sentimento varchar(20) not null,
  qt_delay int not null,
  qt_total int not null
)
*/
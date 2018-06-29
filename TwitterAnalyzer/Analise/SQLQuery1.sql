create procedure prcStatsWordCount
@cd_consulta int,
@inicio datetime,
@fim datetime
as

delete from stats_word_count where cd_consulta = @cd_consulta and dt_inicio = @inicio and dt_fim = @fim


declare @termo varchar(200)
set @termo = (select ds_termo from [dbo].[twitter_consulta] where cd_consulta = @cd_consulta)

if OBJECT_ID('tempdb..##stats_entidades') is not null begin
  drop table ##stats_entidades
end

select dbo.fn_StripCharacters(b.text, '^a-z0-9') as text,COUNT(*) as total
  into ##stats_entidades
  from twitterAnalyzer.dbo.twitter a
    join twitterAnalyzer.dbo.entidade b on b.twitter_id = a.id
where a.consulta_id = 1
  and a.text like '%'+@termo+'%'
  and a.sentimento is not null
  and a.created_at >= '2018-6-26 00:00' 
  and a.created_at <= '2018-6-27 14:00' 
  and b.text not like '%'+@termo+'%'
group by dbo.fn_StripCharacters(b.text, '^a-z0-9')

declare @total int
set @total = (select SUM(total) from ##stats_entidades)
declare @count int
set @count = (select COUNT(*) from ##stats_entidades)

declare @media int
set @media = @total/@count

insert into stats_word_count (cd_consulta,dt_inicio,dt_fim,qt_word,ds_word)
select @cd_consulta,@inicio,@fim, total,text
  from ##stats_entidades
where total > @media


/*
create table stats_word_count
(
   cd_stats_word_count bigint identity(1,1) not null primary key,
   cd_consulta int not null,
   dt_inicio datetime not null,
   dt_fim datetime not null,
   qt_word int not null,
   ds_word varchar(200) not null,
   constraint fk_stats_word_count_consulta foreign key (cd_consulta) references [twitter_consulta](cd_consulta)
)
*/


/*select *
  from stats_users su
where su.statuses_avg > su.statuses_avg_faixa
*/
--(select avg(gu.statuses_count/(DATEDIFF(month,gu.created_at,getdate())+1)) from [user] gu where gu.id <> t.id and (DATEDIFF(month,gu.created_at,getdate())+1) = t.Months_Age)  as statuses_avg_faixa
--TRUNCATE TABLE stats_users

--INSERT INTO stats_users
select top 10 T.*,
      (select avg(gu.statuses_count)/avg(DATEDIFF(month,gu.created_at,getdate())+1) from [user] gu where gu.id <> t.id and (DATEDIFF(month,gu.created_at,getdate())+1) = t.Months_Age)  as statuses_avg_faixa
from (
select a.id,
       a.name,
       a.verified,
       a.protected,
	   (DATEDIFF(month,a.created_at,getdate())+1) as Months_Age,
	   a.lang,
	   a.statuses_count,
	   a.statuses_count / (DATEDIFF(month,a.created_at,getdate())+1) as statuses_avg,
	   a.default_profile,
	   a.friends_count,
	   a.followers_count,
	   a.location,
	   (select count(*) from twitter tw where tw.user_id = a.id) as twittes_count,
	   (select count(*) from twitter tw where tw.user_id = a.id and tw.sentimento = 'POSITIVO') as positive_count,
	   (select count(*) from twitter tw where tw.user_id = a.id and tw.sentimento = 'NEGATIVO') as negative_count,
	   (select count(*) from twitter tw where tw.user_id = a.id and tw.sentimento = 'NEUTRO') as neutral_count
 from [user] a) as T
 where T.Months_Age  = 1

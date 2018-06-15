create procedure prcProcessaTermosInteresesSentimento
as

declare @id_termo uniqueidentifier
--set @id_termo ='115F28DD-4AE9-40FA-9F68-1D25B3A092ED'
declare @search varchar(8000)
--set @search = (select [text] from termointeresse where [id] = @id_termo)


declare cr_termos cursor 
  for select [id],[text] from termointeresse

open cr_termos
FETCH NEXT FROM cr_termos  INTO @id_termo, @search 

WHILE @@FETCH_STATUS = 0  
BEGIN

	if (@search is null)  begin
	  raiserror( 'Termo de interesse vazio ou não encontrado',16,1)
	end

	delete from AnaliseSentimento where id_termo = @id_termo

	insert into AnaliseSentimento
	select min(@id_termo) as id_termo,dt_referencia,
		   sum(case when sentimento = 'POSITIVO' then 1 else 0 end) qt_positivo,
		   sum(case when sentimento = 'NEUTRO' then 1 else 0 end) qt_neutro,
		   sum(case when sentimento = 'NEGATIVO' then 1 else 0 end) qt_negativo
	from(
		select @search as termo,
			   cast(a.created_at as date) as dt_referencia,
			   sentimento
		  from twitter a
		where a.text like '%'+ @search + '%'
		  and sentimento = 'POSITIVO'
		union all
		select @search as termo,
			   cast(a.created_at as date) as dt_referencia,
			   sentimento
		  from twitter a
		where a.text like '%'+ @search + '%'
		  and sentimento = 'NEGATIVO'
		union all
		select @search as termo,
			   cast(a.created_at as date) as dt_referencia,
			   sentimento
		  from twitter a
		where a.text like '%'+ @search + '%'
		  and sentimento = 'NEUTRO'
	) as T
	group by dt_referencia
	FETCH NEXT FROM cr_termos  INTO @id_termo, @search 
end
create table Portador
(
   cd_portador int identity(1,1) not null primary key,
   ds_cpf varchar(20) null,
   ds_portador varchar(200) not null
)
go
create index idx_portador on portador (ds_cpf,ds_portador)
go
insert into Portador(ds_portador,ds_cpf)
select ds_portador,TRIM(ds_cpf_portador)
  from [ods_cartao_pagamento_federal_CPGF]
group by ds_portador,TRIM(ds_cpf_portador)

update Portador set ds_cpf = 'Não Informado' where ds_cpf is null or LEN(ds_cpf) = 0

go
select a.cd_orgao_superior,a.ds_orgao_superior
  from [ods_cartao_pagamento_federal_CPGF] a
where not exists(select 1 from orgao_superior b where b.cd_orgao = a.cd_orgao_superior and b.ds_orgao = a.ds_orgao_superior)
go
insert into orgao_subordinado (cd_orgao,ds_orgao)
select distinct a.cd_orgao_subordinado,a.ds_orgao_subordinado
  from [ods_cartao_pagamento_federal_CPGF] a
where not exists(select 1 from orgao_subordinado b where b.cd_orgao = a.cd_orgao_subordinado and b.ds_orgao = a.ds_orgao_subordinado)
go
insert into unidade_gestora (cd_unidade,ds_unidade)
select distinct a.cd_unidade_gestora,A.ds_unidade_gestora
  from [ods_cartao_pagamento_federal_CPGF] a
where not exists(select 1 from unidade_gestora b where b.cd_unidade = a.cd_unidade_gestora and b.ds_unidade = a.ds_unidade_gestora)
go
insert favorecido (nr_documento,ds_favorecido)
select distinct   A.nr_documento_favorecido,A.ds_favorecido
  from [ods_cartao_pagamento_federal_CPGF] a
where not exists(select 1 from favorecido b where b.[nr_documento] = a.nr_documento_favorecido and b.ds_favorecido = a.ds_favorecido)
go
insert transacao (ds_transacao)
select distinct a.ds_transacao
  from [ods_cartao_pagamento_federal_CPGF] a
where not exists(select 1 from transacao b where b.ds_transacao = a.ds_transacao)
go
select 
       os.cd_orgao_superior,
       osub.cd_orgao_subordinado,
	   u.cd_unidade_gestora,
	   a.nr_ano,
	   a.nr_mes,
	   p.cd_portador,
	   fav.cd_favorecido,
	   t.cd_transacao,
       case 
	     when ISDATE(RIGHT(a.dt_transacao,4)+'-'+SUBSTRING(a.dt_transacao,4,2)+'-'+left(a.dt_transacao,2)) = 1 then
	       convert(datetime,RIGHT(a.dt_transacao,4)+'-'+SUBSTRING(a.dt_transacao,4,2)+'-'+left(a.dt_transacao,2)) 
		 else '1-1-1' end as dt_transacao,
       convert(money,replace(replace([vl_transacao],'.',''),',','.')) as vl_transacao
into fato_cartao_pagamento_federal
from [ods_cartao_pagamento_federal_CPGF] a
  left join orgao_superior os on os.cd_orgao = A.cd_orgao_superior and os.ds_orgao = A.ds_orgao_superior
  left join orgao_subordinado osub on osub.cd_orgao = A.cd_orgao_subordinado and osub.ds_orgao = A.ds_orgao_subordinado
  left join unidade_gestora u on u.cd_unidade = A.cd_unidade_gestora and u.ds_unidade = A.ds_unidade_gestora
  left join favorecido fav on fav.nr_documento = a.nr_documento_favorecido and fav.ds_favorecido = a.ds_favorecido
  left join transacao t on t.ds_transacao = a.ds_transacao
  left join Portador p on p.ds_cpf = trim(a.ds_cpf_portador) and p.ds_portador = A.ds_portador




select top 10 * from ods_CPDC

insert into orgao_superior(cd_orgao,ds_orgao)
select distinct cd_orgao_superior,ds_orgao_superior 
from ods_CPDC a
where not exists(select 1 from orgao_superior b where A.cd_orgao_superior = B.cd_orgao and A.ds_orgao_superior = B.ds_orgao)

insert into orgao_subordinado(cd_orgao,ds_orgao)
select distinct cd_orgao_subordinado,ds_orgao_subordinado
from ods_CPDC a
where not exists(select 1 from orgao_subordinado b 
                   where A.cd_orgao_subordinado = B.cd_orgao 
				     and A.ds_orgao_subordinado = B.ds_orgao)

insert into unidade_gestora(cd_unidade,ds_unidade)
select distinct cd_unidade_gestora,ds_unidade_gestora
from ods_CPDC a
where not exists(select 1 from unidade_gestora b 
                   where A.cd_unidade_gestora = B.cd_unidade 
				     and A.ds_unidade_gestora = B.ds_unidade)

insert into Portador(ds_cpf,ds_portador)
select distinct ds_cpf_portador,ds_nome_portador
from ods_CPDC a
where not exists(select 1 from Portador b 
                   where b.ds_cpf = a.ds_cpf_portador 
				     and b.ds_portador = a.ds_nome_portador)

insert into favorecido(nr_documento,ds_favorecido)
select distinct ds_documento_favorecido,ds_favorecido
from ods_CPDC a
where not exists(select 1 from favorecido b 
                   where b.ds_favorecido = a.ds_favorecido
				     and b.nr_documento = a.ds_documento_favorecido)

insert into Executor(ds_executor)
select distinct ds_executor_despesa
from ods_CPDC a
where not exists(select 1 from Executor b
                   where b.ds_executor = a.ds_favorecido)

insert convenente(cd_convenio,nr_convenente,ds_convenente)
select distinct ds_convenio,
                cd_convenio,
				ds_nome_convenente
from ods_CPDC a
where not exists(select 1 
                   from convenente b
                   where b.cd_convenio   = a.ds_convenio
				     and b.nr_convenente = a.cd_convenio
					 and b.ds_convenente = a.ds_convenio
				   )

insert into transacao (ds_transacao)
select distinct a.ds_transacao
  from ods_CPDC a
where not exists(select 1  
                   from transacao b
				   where b.ds_transacao = a.ds_transacao)

--Cartao de Pagamento da Defesa Civil (CPDC)

select b.cd_orgao_superior,
       c.cd_orgao_subordinado,
	   d.cd_unidade_gestora,
	   a.nr_ano,
	   a.nr_mes,
	   e.cd_portador,
	   f.cd_favorecido,
	   g.cd_executor,
       h.cd_convenente,
	   case when a.ds_repasse = 'SIM' then 'TRUE' else 'FALSE' end fl_repasse,
	   i.cd_transacao,
	   convert(datetime,RIGHT(a.dt_transacao,4)+'-'+SUBSTRING(a.dt_transacao,4,2)+'-'+left(a.dt_transacao,2))  as dt_transacao,
	   vl_transacao
  into fato_cartao_pagamento_defesa_civil 
  from ods_CPDC a
  left join orgao_superior b on a.cd_orgao_superior = b.cd_orgao and a.ds_orgao_superior = b.ds_orgao
  left join orgao_subordinado c on c.cd_orgao = a.cd_orgao_subordinado and c.ds_orgao = a.ds_orgao_subordinado
  left join unidade_gestora d on d.cd_unidade = a.cd_unidade_gestora and d.ds_unidade = a.ds_unidade_gestora
  left join Portador e on e.ds_cpf = a.ds_cpf_portador and e.ds_portador = a.ds_nome_portador
  left join favorecido f on f.nr_documento = a.ds_documento_favorecido and f.ds_favorecido = a.ds_favorecido
  left join Executor g on g.ds_executor = a.ds_executor_despesa
  left join convenente h on h.cd_convenio = a.ds_convenio and h.nr_convenente = a.cd_convenio and h.ds_convenente = a.ds_nome_convenente
  left join transacao i on i.ds_transacao = a.ds_transacao

  

  

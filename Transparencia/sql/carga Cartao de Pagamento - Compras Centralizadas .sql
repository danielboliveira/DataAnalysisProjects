--Orgao Superior
create table orgao_superior
(
  cd_orgao_superior int identity(1,1) primary key not null,
  cd_orgao int not null,
  ds_orgao varchar(200) not null
)
go

insert into orgao_superior (cd_orgao,ds_orgao)
select distinct cd_orgao_superior,nm_orgao_superior
from merged a
where not exists(select 1 from orgao_superior os where os.cd_orgao = A.cd_orgao_superior and os.ds_orgao = A.nm_orgao_superior)

select * from orgao_superior
go
--Orgão
create table orgao_subordinado
(
  cd_orgao_subordinado int identity(1,1) primary key not null,
  cd_orgao int not null,
  ds_orgao varchar(200) not null
)
go
insert into orgao_subordinado (cd_orgao,ds_orgao)
select distinct cd_orgao,nm_orgao
from merged a
where not exists(select 1 from orgao_subordinado os where os.cd_orgao = A.cd_orgao and os.ds_orgao = A.nm_orgao)
go
select * from orgao_subordinado
go
create table unidade_gestora
(
  cd_unidade_gestora int identity(1,1) primary key not null,
  cd_unidade int not null,
  ds_unidade varchar(200) not null
)
create index idx_unidade_gestora on unidade_gestora(cd_unidade,ds_unidade)
go
insert into unidade_gestora(cd_unidade,ds_unidade)
select distinct cd_unidade_gestora,ds_unidade_gestora
from merged a
where not exists(select 1 from unidade_gestora os where os.cd_unidade = A.cd_unidade_gestora and os.ds_unidade = A.ds_unidade_gestora)
go
create table tipo_aquisicao
(
  cd_tipo_aquisicao int identity(1,1) primary key not null,
  ds_tipo_aquisicao varchar(200) not null
)
go
insert into tipo_aquisicao(ds_tipo_aquisicao)
select distinct UPPER(ds_tipo_aquisicao)
from merged a
where not exists(select 1 from  tipo_aquisicao os where os.ds_tipo_aquisicao = a.ds_tipo_aquisicao)
go
select * from tipo_aquisicao
go
create table favorecido
(
  cd_favorecido bigint identity(1,1) not null primary key,
  nr_documento varchar(20) not null,
  ds_favorecido varchar(200) not null
)
go
insert into favorecido(nr_documento,ds_favorecido)
select distinct nr_documento_favorecido, UPPER(ds_favorecido)
from merged a
where not exists(select 1 from favorecido os where os.nr_documento = a.nr_documento_favorecido and os.ds_favorecido = A.ds_favorecido)
go
select * from favorecido
go
create table transacao
(
   cd_transacao int not null identity(1,1) primary key,
   ds_transacao varchar(200) not null
)
go
insert into transacao(ds_transacao)
select distinct UPPER(ds_transacao)
from merged a
where not exists(select 1 from transacao os where os.ds_transacao = a.ds_transacao)
go
select * from transacao

select os.cd_orgao_superior,
              osub.cd_orgao_subordinado,
			  u.cd_unidade_gestora,
			  a.nr_ano,
			  a.nr_mes,
			  ta.cd_tipo_aquisicao,
			  fav.cd_favorecido,
			  t.cd_transacao,
              convert(datetime,RIGHT(a.dt_transacao,4)+'-'+SUBSTRING(a.dt_transacao,4,2)+'-'+left(a.dt_transacao,2)) as dt_transacao,
              convert(money,replace(replace([vl_transacao],'.',''),',','.')) as vl_transacao
into ods_compras_cartao_centralizadas
from merged a
  left join orgao_superior os on os.cd_orgao = A.cd_orgao_superior and os.ds_orgao = A.nm_orgao_superior
  left join orgao_subordinado osub on osub.cd_orgao = A.cd_orgao and osub.ds_orgao = A.nm_orgao
  left join unidade_gestora u on u.cd_unidade = A.cd_unidade_gestora and u.ds_unidade = A.ds_unidade_gestora
  left join tipo_aquisicao ta on ta.ds_tipo_aquisicao = a.ds_tipo_aquisicao
  left join favorecido fav on fav.nr_documento = a.nr_documento_favorecido and fav.ds_favorecido = a.ds_favorecido
  left join transacao t on t.ds_transacao = a.ds_transacao




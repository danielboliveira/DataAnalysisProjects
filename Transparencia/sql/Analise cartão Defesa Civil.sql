--Evolução anual dos gastos
select a.nr_ano,
       COUNT(*) as qt_transacoes,
       sum(a.vl_transacao) as vl_total,
	   AVG(a.vl_transacao) as vl_medio,
	   MIN(a.vl_transacao) as vl_minimo,
	   MAX(a.vl_transacao) as vl_maximo
from fato_cartao_pagamento_defesa_civil  a
group by a.nr_ano

select a.nr_ano,
       COUNT(*) as qt_transacoes,
       sum(a.vl_transacao) as vl_total,
	   AVG(a.vl_transacao) as vl_medio,
	   MIN(a.vl_transacao) as vl_minimo,
	   MAX(a.vl_transacao) as vl_maximo
from fato_cartao_pagamento_defesa_civil  a
where vl_transacao > 0
group by a.nr_ano

select a.nr_ano,
       COUNT(*) as qt_transacoes,
       sum(a.vl_transacao) as vl_total,
	   AVG(a.vl_transacao) as vl_medio,
	   MIN(a.vl_transacao) as vl_minimo,
	   MAX(a.vl_transacao) as vl_maximo
from fato_cartao_pagamento_defesa_civil  a
where vl_transacao < 0
group by a.nr_ano

--Favorecidos
select  c.ds_executor,b.ds_favorecido,a.nr_ano,a.nr_mes, SUM(a.vl_transacao) as vl_total
  from fato_cartao_pagamento_defesa_civil a
    join favorecido b on b.cd_favorecido = a.cd_favorecido
	join Executor c on c.cd_executor = a.cd_executor
--where a.vl_transacao > 0
group by c.ds_executor, b.ds_favorecido,a.nr_ano,a.nr_mes
order by 3 desc

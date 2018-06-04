select text,sentimento,sum(total)
from EntidadesOrganico
group by text,sentimento
order by sum(total) desc
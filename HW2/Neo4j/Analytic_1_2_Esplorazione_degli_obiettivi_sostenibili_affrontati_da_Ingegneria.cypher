//Analytic 1.2 Esplorazione degli obiettivi sostenibili affrontati da 'Ingegneria'
MATCH (o:ObiettiviSostenibili)<-[r:FOCUSED_ON]-(p:Project)-[t:BELONGS_TO]->(a:Ambiti {Field: '40 Engineering'})
RETURN a, o, COLLECT(p) AS progetti, count(p) AS N_progetti 
ORDER BY N_progetti DESC
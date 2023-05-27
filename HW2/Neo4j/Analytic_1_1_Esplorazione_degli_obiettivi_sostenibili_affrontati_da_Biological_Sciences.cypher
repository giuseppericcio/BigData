//Analytic 1.1 Esplorazione degli obiettivi sostenibili affrontati da 'Biological Sciences'
MATCH (o:ObiettiviSostenibili)<-[r:FOCUSED_ON]-(p:Project)-[t:BELONGS_TO]->(a:Ambiti {Field: '31 Biological Sciences'})
RETURN a, o, COLLECT(p) AS progetti, count(p) AS N_progetti 
ORDER BY N_progetti DESC
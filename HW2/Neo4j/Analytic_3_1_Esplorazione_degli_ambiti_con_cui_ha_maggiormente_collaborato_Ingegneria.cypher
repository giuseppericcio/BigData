//Analytic 3.1 Esplorazione degli ambiti con cui ha maggiormente collaborato 'Ingegneria'
MATCH (a:Ambiti)<-[r:BELONGS_TO]-(p:Project)
WHERE exists((p:Project)-[:BELONGS_TO]->(:Ambiti {Field: '40 Engineering'})) AND a.Field =~ '^\\d{2}\\b.*' AND p.Start_Year > 2020
RETURN a, COLLECT(p) AS progetti, count(p) AS N_progetti 
ORDER BY N_progetti DESC
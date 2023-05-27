//Analytic 4.1 Esplorazione degli ambiti con cui ha maggiormente collaborato 'Ingegneria Aereospaziale'
MATCH (a:Ambiti)<-[r:BELONGS_TO]-(p:Project)
WHERE exists((p:Project)-[:BELONGS_TO]->(:Ambiti {Field: '4001 Aerospace Engineering'})) AND a.Field =~ '^\\d{4}\\b.*'
RETURN a, COLLECT(p) AS progetti, count(p) AS N_progetti 
ORDER BY N_progetti DESC
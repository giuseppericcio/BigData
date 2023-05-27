//Analytic 1.2 Grafo dei primi 5
match (a:Ambiti)<-[r:BELONGS_TO]-(p:Project)
where a.Field =~ '^\\d{2}\\b.*'
return a, count(a) As NumeroAmbiti
order by NumeroAmbiti DESC
LIMIT 5
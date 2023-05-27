//Analytic 2: TOP 10 Ambiti di Ricerca specifici per numero progetti
MATCH (a:Ambiti)-[:BELONGS_TO]-(p:Project)
WHERE a.Field =~ '^\\d{4}\\b.*'
WITH a, COUNT(p) AS numProjects
ORDER BY numProjects DESC
LIMIT 10
MATCH (a)-[:BELONGS_TO]-(p:Project)
RETURN a, p
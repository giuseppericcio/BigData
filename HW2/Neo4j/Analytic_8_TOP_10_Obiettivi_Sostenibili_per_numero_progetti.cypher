//Analytic 8: TOP 10 Obiettivi Sostenibili per numero progetti
MATCH (s:ObiettiviSostenibili)-[:FOCUSED_ON]-(p:Project)
WITH s, COUNT(p) AS numProjects
ORDER BY numProjects DESC
LIMIT 10
MATCH (s)-[:FOCUSED_ON]-(p:Project)
RETURN s, p
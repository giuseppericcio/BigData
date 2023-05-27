//Analytic 6: TOP 10 Ambiti di ricerca generici per numero progetti dal 1980 al 2001
MATCH (d:Dipartimenti)<-[:REALIZED_BY]-(p:Project)-[:BELONGS_TO]->(a:Ambiti)
WHERE a.Field =~ '^\\d{2}\\b.*' AND (p.Start_Year >= 1980 AND p.Start_Year < 2002)
RETURN a, d, collect(distinct(p)), COUNT(distinct(p)) as ProjectCount
ORDER BY ProjectCount DESC
LIMIT 10
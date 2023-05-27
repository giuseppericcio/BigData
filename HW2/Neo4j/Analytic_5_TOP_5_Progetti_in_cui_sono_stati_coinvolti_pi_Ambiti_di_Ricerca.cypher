//Analytic 5: TOP 5 Progetti in cui sono stati coinvolti più Ambiti di Ricerca 
MATCH (p:Project)-[r:BELONGS_TO]->(a:Ambiti)
WHERE a.Field =~ '^\\d{4}\\b.*'
RETURN p, collect(distinct(a)) AS AmbitiTitoli, COUNT(distinct(a)) AS NumAmbiti
ORDER BY NumAmbiti DESC
LIMIT 5
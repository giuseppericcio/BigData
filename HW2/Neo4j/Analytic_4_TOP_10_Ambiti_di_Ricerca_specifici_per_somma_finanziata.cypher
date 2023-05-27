//Analytic 4: TOP 10 Ambiti di Ricerca specifici per somma finanziata
MATCH (p:Project)-[:BELONGS_TO]->(a:Ambiti)
WITH a, sum(p.Funding_Amount_in_EUR) as TotalFunding
WHERE a.Field =~ '^\\d{4}\\b.*'
RETURN a.Field as AmbitoRicerca, TotalFunding
ORDER BY TotalFunding DESC
LIMIT 10
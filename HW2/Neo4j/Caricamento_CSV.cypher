// Caricamento CSV
LOAD CSV WITH HEADERS FROM 'file:///Dataset_Projects_Unina_with_Topic.csv' AS row
MERGE (p:Project {Title_translated: row.Title_translated})
SET p.Funding_Amount_in_EUR = toFloat(row.Funding_Amount_in_EUR),
    p.Start_Year = toInteger(row.Start_Year),
    p.Topic = row.Topic

FOREACH (field IN SPLIT(row.Fields_of_Research_ANZSRC_2020, ';') |
    MERGE (a:Ambiti {Field: TRIM(field)})
    CREATE (p)-[:BELONGS_TO]->(a)
)

FOREACH (sust IN SPLIT(row.Sustainable_Development_Goals, ';') |
    MERGE (s:ObiettiviSostenibili {Sust: TRIM(sust)})
    CREATE (p)-[:FOCUSED_ON]->(s)
)

FOREACH (dip IN SPLIT(row.Units_of_Assessment, ';') |
    MERGE (d:Dipartimenti {Dip: TRIM(dip)})
    CREATE (p)-[:REALIZED_BY]->(d)
)
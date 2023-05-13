-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## HW1 - Notebook: Analytics in HIVE
-- MAGIC
-- MAGIC Il notebook mostra una serie di Analytics sui progetti fatti negli anni passati e in corso dall'Università di Napoli, Federico II
-- MAGIC
-- MAGIC Questo notebook è scritto in **HIVEQL**

-- COMMAND ----------

-- Visualizzazione tabellare del dataset completo.
SELECT * from dataset_unina_research;

-- COMMAND ----------

-- Schema del dataset in esame.
DESCRIBE dataset_unina_research;

-- COMMAND ----------

-- Calcolare la somma finanziata tra i 5 dipartimenti con più progetti nella Federico II.
SELECT Dipartimento, Somma_Finanziata_EUR
FROM (SELECT trim(Dipartimento) AS Dipartimento, count(*) AS Num_progetti, SUM(Funding_Amount_in_EUR) AS Somma_Finanziata_EUR
      FROM (SELECT explode(split(Units_of_Assessment,'; ')) AS Dipartimento, Funding_Amount_in_EUR
            FROM dataset_unina_research)
      WHERE Dipartimento IS NOT NULL AND Funding_Amount_in_EUR IS NOT NULL
      GROUP BY Dipartimento
      ORDER BY Num_progetti DESC
      LIMIT 5)
ORDER BY int(Somma_Finanziata_EUR) DESC;

-- COMMAND ----------

-- Le 10 tematiche più trattate negli ultimi 10 anni dalla Federico II.
SELECT trim(Ambito_di_Ricerca) AS Ambito_di_Ricerca, count(*) AS Numero_Progetti
FROM (SELECT explode(split(Fields_of_Research_ANZSRC_2020,'; ')) AS Ambito_di_Ricerca, End_Year
      FROM dataset_unina_research)
WHERE Ambito_di_Ricerca IS NOT NULL AND End_Year >= '2012'
GROUP BY Ambito_di_Ricerca
ORDER BY Numero_Progetti DESC
LIMIT 10;

-- COMMAND ----------

-- Le 100 istituzioni con cui ha collaborato di più la Federico II.
SELECT trim(Istituzione) AS Istituzione, count(*) AS Numero_Collaborazioni
FROM (SELECT explode(split(Research_Organization_original, "; ")) AS Istituzione
      FROM dataset_unina_research)
WHERE Istituzione != "University of Naples Federico II" 
GROUP BY Istituzione
ORDER BY Numero_Collaborazioni DESC
LIMIT 100;

-- COMMAND ----------

-- TOP 10 dipartimenti con progetti ancora in corso.
SELECT trim(Dipartimento) AS Dipartimento, count(*) AS Numero_Progetti_In_Corso 
FROM (SELECT explode(split(Units_of_Assessment, '; ')) AS Dipartimento 
      FROM dataset_unina_research 
      WHERE End_Date > CURRENT_DATE())
GROUP BY Dipartimento
ORDER BY Numero_Progetti_In_Corso DESC
LIMIT 10;

-- COMMAND ----------

-- Somma Finanziata dalla Federico II nelle ricerche negli ultimi 20 anni.
SELECT Start_Year AS Anno_Inizio, SUM(Funding_Amount_in_EUR) AS Somma_Finanziata_EUR 
FROM dataset_unina_research
WHERE Start_Year > '2001'
GROUP BY Start_Year
ORDER BY Start_Year DESC;

-- COMMAND ----------

-- Somma e numero progetti della federico II nel 2020.
SELECT Campi_di_ricerca, count(Campi_di_ricerca) AS Numero_progetti, SUM(Funding_Amount_in_EUR) AS Somma_Finanziata_Totale
FROM (SELECT explode(split(Fields_of_Research_ANZSRC_2020, "; ")) AS Campi_di_ricerca, Funding_Amount_in_EUR
      FROM dataset_unina_research 
      WHERE Start_Year = '2020')
GROUP BY Campi_di_ricerca
ORDER BY Numero_progetti DESC;

-- COMMAND ----------

-- Somma Finanziata dalla Federico II nelle ricerche sul Cancro negli ultimi 20 anni.
SELECT Start_Year AS Anno_Inizio, SUM(Funding_Amount_in_EUR) AS Somma_Finanziata_Cancro_EUR 
FROM dataset_unina_research
WHERE Cancer_Types IS NOT NULL AND (Start_Year IS NOT NULL AND Start_Year > '2001')
GROUP BY Start_Year
ORDER BY Start_Year DESC;

-- COMMAND ----------

-- Le parole chiavi più utilizzate nei titoli dei progetti.
SELECT trim(Topic_Titolo) AS Topic_Titolo, count(*) AS Conteggio
FROM (SELECT explode(split(regexp_replace(lower(Title_translated), '[\\p{Punct},\\p{Cntrl}]', ''), ' ')) AS Topic_Titolo
      FROM dataset_unina_research)
WHERE Topic_Titolo NOT IN (SELECT * FROM stopwords) AND Topic_Titolo != ""
GROUP BY Topic_Titolo
ORDER BY Conteggio DESC, Topic_Titolo ASC
LIMIT 100;

-- COMMAND ----------

-- I 10 Paesi esteri con cui ha maggiormente collaborato l'Università.
SELECT Paese, count(Paese) AS Numero_Collaborazioni
FROM (SELECT DISTINCT explode(split(trim(Country_of_Research_organization), "; ")) AS Paese, Title_translated
      FROM dataset_unina_research)
WHERE Paese != "Italy"
GROUP BY Paese
ORDER BY Numero_Collaborazioni DESC
LIMIT 10;

-- COMMAND ----------

-- Gli ambiti di ricerca con progetti di maggiore durata.
SELECT Title_translated AS Titolo_Progetto, trim(Dipartimento) AS Dipartimento, (End_Year - Start_Year) AS Durata_Progetto_in_Anni 
FROM (SELECT explode(split(Units_of_Assessment, ';')) AS Dipartimento, End_Year, Start_Year, Title_translated 
      FROM dataset_unina_research)
GROUP BY Titolo_Progetto, Dipartimento, Durata_Progetto_in_Anni
ORDER BY Durata_Progetto_in_Anni DESC
LIMIT 10;

-- COMMAND ----------

-- Capacità di gestione delle risorse finanziare dei progetti negli ultimi 20 anni.
SELECT Start_Year AS Anno_Inizio, (int(Numero_progetti)/int(Somma_Finanziata_EUR)) AS Capacita_Progetti, (int(Somma_Finanziata_EUR)/int(Numero_progetti)) AS Fondi_media_Progetti
FROM (SELECT Start_Year, SUM(Funding_Amount_in_EUR) AS Somma_Finanziata_EUR, COUNT(*) AS Numero_Progetti
      FROM dataset_unina_research
      GROUP BY Start_Year)
WHERE Start_Year IS NOT NULL 
GROUP BY Start_Year, Somma_Finanziata_EUR, Numero_Progetti
ORDER BY Start_Year DESC
LIMIT 22;

-- COMMAND ----------

-- Numero di progetti di ricerca per ogni obiettivo di sviluppo sostenibile.
SELECT trim(Obiettivo_Sostenibilita) AS Obiettivo_Sostenibilita, COUNT(*) AS Numero_Progetti, SUM(Funding_Amount_in_EUR) AS Somma_finanziata
FROM (SELECT explode(split(Sustainable_Development_Goals, "; ")) AS Obiettivo_Sostenibilita, Funding_Amount_in_EUR
      FROM dataset_unina_research)
WHERE Obiettivo_Sostenibilita IS NOT NULL
GROUP BY Obiettivo_Sostenibilita
ORDER BY Numero_Progetti DESC;

-- COMMAND ----------

-- Numero di progetti di ricerca per ogni tipo di cancro.
SELECT trim(Tipo_Cancro) AS Tipo_Cancro, COUNT(*) AS Numero_Progetti
FROM (SELECT explode(split(Cancer_types, "; ")) AS Tipo_Cancro
      FROM dataset_unina_research)
WHERE Tipo_Cancro IS NOT NULL AND Tipo_Cancro != 'Not Site-Specific Cancer'
GROUP BY Tipo_Cancro
ORDER BY Numero_Progetti DESC
LIMIT 25;

-- COMMAND ----------

-- Tipi di malattie, possibili cure e gli obiettivi perseguiti.
SELECT trim(Cat_Medica) AS Settore_Medico, count(*) AS Numero_Progetti, SUM(Funding_Amount_in_EUR) AS Somma_Finanziata_EUR
FROM (SELECT explode(split(RCDC_Categories, "; ")) AS Cat_Medica, Funding_Amount_in_EUR
      FROM dataset_unina_research)
WHERE Cat_Medica IS NOT NULL
GROUP BY Cat_Medica
ORDER BY int(Somma_Finanziata_EUR) DESC, Numero_Progetti DESC
LIMIT 10;

dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

progetti = FOREACH dataset_unina_research GENERATE Start_Year AS Anno_Inizio, Funding_Amount_in_EUR;

progetti_filtrati = FILTER progetti BY Anno_Inizio IS NOT NULL;

progetti_contati = GROUP progetti_filtrati BY Anno_Inizio;
progetti_contati_final = FOREACH progetti_contati GENERATE group AS Anno_Inizio, COUNT(progetti_filtrati) AS Numero_Progetti, SUM(progetti_filtrati.Funding_Amount_in_EUR) AS Somma_Finanziata_EUR;

capacita_progetti = FOREACH progetti_contati_final GENERATE Anno_Inizio, ((DOUBLE)Numero_Progetti/(DOUBLE)Somma_Finanziata_EUR) AS Capacita_Progetti, ((DOUBLE)Somma_Finanziata_EUR/(DOUBLE)Numero_Progetti) AS Fondi_Media_Progetti;

capacita_progetti_ordinati = ORDER capacita_progetti BY Anno_Inizio DESC;
capacita_progetti_finali = LIMIT capacita_progetti_ordinati 22;

STORE capacita_progetti_finali INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/CapacitaProgetti.txt' USING PigStorage(',');
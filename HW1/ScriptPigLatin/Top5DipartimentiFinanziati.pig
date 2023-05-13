dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

dipartimenti = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Units_of_Assessment), ';')) AS Dipartimento, Funding_Amount_in_EUR;

dipartimenti_filtrati = FILTER dipartimenti BY Dipartimento IS NOT NULL AND Funding_Amount_in_EUR IS NOT NULL;

dipartimenti_contati = GROUP dipartimenti_filtrati BY Dipartimento;
dipartimenti_contati_final = FOREACH dipartimenti_contati GENERATE group AS Dipartimento, COUNT(dipartimenti_filtrati) AS Numero_Progetti, SUM(dipartimenti_filtrati.Funding_Amount_in_EUR) AS Somma_Finanziata_EUR;

dipartimenti_ordinati = ORDER dipartimenti_contati_final BY Numero_Progetti DESC;
top_5_dipartimenti = LIMIT dipartimenti_ordinati 5;

top_5_dipartimenti_finanziati = FOREACH top_5_dipartimenti GENERATE Dipartimento AS Dipartimento, Somma_Finanziata_EUR AS Somma_Finanziata_EUR;
top_5_dipartimenti_finanziati_final = ORDER top_5_dipartimenti_finanziati BY Somma_Finanziata_EUR DESC;

STORE top_5_dipartimenti_finanziati_final INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Top5DipartimentiFinanziati.txt' USING PigStorage(',');
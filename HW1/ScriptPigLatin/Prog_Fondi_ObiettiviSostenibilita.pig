dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

obiettivi_sostenibilita = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Sustainable_Development_Goals), ';')) AS Obiettivo_Sostenibilita, Funding_Amount_in_EUR;

obiettivi_sostenibilita_filtrati = FILTER obiettivi_sostenibilita BY Obiettivo_Sostenibilita IS NOT NULL;

obiettivi_sostenibilita_contati = GROUP obiettivi_sostenibilita_filtrati BY Obiettivo_Sostenibilita;
obiettivi_sostenibilita_contati_final = FOREACH obiettivi_sostenibilita_contati GENERATE group AS Obiettivo_Sostenibilita, COUNT(obiettivi_sostenibilita_filtrati) AS Numero_Progetti, SUM(obiettivi_sostenibilita_filtrati.Funding_Amount_in_EUR) AS Somma_Finanziata;

obiettivi_sostenibilita_ordinati = ORDER obiettivi_sostenibilita_contati_final BY Numero_Progetti DESC;

STORE obiettivi_sostenibilita_ordinati INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Prog_Fondi_ObiettiviSostenibilita.txt' USING PigStorage(',');
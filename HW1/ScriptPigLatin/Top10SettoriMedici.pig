dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

settori_medici = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(RCDC_Categories), ';')) AS Cat_Medica, Funding_Amount_in_EUR;

settori_medici_filtrati = FILTER settori_medici BY Cat_Medica IS NOT NULL;

settori_medici_contati = GROUP settori_medici_filtrati BY Cat_Medica;
settori_medici_contati_final = FOREACH settori_medici_contati GENERATE group AS Settore_Medico, COUNT(settori_medici_filtrati) AS Numero_Progetti, SUM(settori_medici_filtrati.Funding_Amount_in_EUR) AS Somma_Finanziata_EUR;

settori_medici_ordinati = ORDER settori_medici_contati_final BY Somma_Finanziata_EUR DESC, Numero_Progetti DESC;
settori_medici_finali = LIMIT settori_medici_ordinati 10;

STORE settori_medici_finali INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Top10SettoriMedici.txt' USING PigStorage(',');
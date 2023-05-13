dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

dipartimenti = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Units_of_Assessment), ';')) AS Dipartimento, End_Date;

dipartimenti_filtrati = FILTER dipartimenti BY End_Date > ToString(CurrentTime());

dipartimenti_contati = GROUP dipartimenti_filtrati BY Dipartimento;
dipartimenti_contati_final = FOREACH dipartimenti_contati GENERATE group AS Dipartimento, COUNT(dipartimenti_filtrati) AS Numero_Progetti_In_Corso;

dipartimenti_ordinati = ORDER dipartimenti_contati_final BY Numero_Progetti_In_Corso DESC, Dipartimento DESC;
dipartimenti_finali = LIMIT dipartimenti_ordinati 10;

STORE dipartimenti_finali INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Top10DipartimentiInCorso.txt' USING PigStorage(',');
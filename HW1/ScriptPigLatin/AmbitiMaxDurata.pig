dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:int, End_Date:chararray, End_Year:int, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

dipartimenti = FOREACH dataset_unina_research GENERATE Title_translated, FLATTEN(TOKENIZE(TRIM(Units_of_Assessment), ';')) AS Dipartimento, End_Year, Start_Year; 

dipartimenti_raggruppati = GROUP dipartimenti BY (Title_translated, Dipartimento);

dipartimenti_conta = FOREACH dipartimenti_raggruppati GENERATE group AS Titolo_progetto, (MAX(dipartimenti.End_Year) - MIN(dipartimenti.Start_Year)) AS Durata_Progetto_in_Anni;

dipartimenti_ordinati = ORDER dipartimenti_conta BY Durata_Progetto_in_Anni DESC, Titolo_progetto DESC;

limite_dipartimenti = LIMIT dipartimenti_ordinati 10;

STORE limite_dipartimenti INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/AmbitiMaxDurata.txt' USING PigStorage(',');
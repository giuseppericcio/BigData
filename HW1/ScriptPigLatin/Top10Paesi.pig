dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2525:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

paesi = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Country_of_Research_organization), ';')) AS Paese, Title_translated;

paesi_distinct = DISTINCT paesi;

paesi_filtrati = FILTER paesi_distinct BY Paese != 'Italy';
paesi_grp = GROUP paesi_filtrati BY Paese;

paese_contati = FOREACH paesi_grp GENERATE group AS Paese, COUNT(paesi_filtrati) AS Numero_Collaborazioni;

paese_ordinati = ORDER paese_contati BY Numero_Collaborazioni DESC;
top_10_paesi = LIMIT paese_ordinati 10;

STORE top_10_paesi INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Top10Paesi.txt' USING PigStorage(',');
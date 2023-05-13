dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

campi = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Fields_of_Research_ANZSRC_2020), ';')) AS Campi_di_ricerca, Funding_Amount_in_EUR, Start_Year;

campi_filtrati = FILTER campi BY Start_Year = '2020';

campi_raggruppati = GROUP campi_filtrati BY Campi_di_ricerca;

progetti = FOREACH campi_raggruppati GENERATE group AS Campi_di_ricerca, SUM(campi_filtrati.Funding_Amount_in_EUR) AS Somma_Finanziata_Totale, COUNT(campi_filtrati) AS Numero_Progetti;

progetti_ordinati = ORDER progetti BY Numero_Progetti DESC;

STORE progetti_ordinati INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Somma_Progetti2020.txt' USING PigStorage(',');
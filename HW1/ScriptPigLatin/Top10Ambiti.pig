dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

ambiti_di_ricerca = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Fields_of_Research_ANZSRC_2020), ';')) AS Ambito_di_Ricerca, End_Year;

ambiti_di_ricerca_filtrati = FILTER ambiti_di_ricerca BY Ambito_di_Ricerca IS NOT NULL AND End_Year >= '2012';

conta_progetti = GROUP ambiti_di_ricerca_filtrati BY Ambito_di_Ricerca;
numero_progetti = FOREACH conta_progetti GENERATE group AS Ambito_di_Ricerca, COUNT(ambiti_di_ricerca_filtrati) AS Numero_Progetti;

top_10_ambiti = ORDER numero_progetti BY Numero_Progetti DESC, Ambito_di_Ricerca ASC;
limite_top_10_ambiti = LIMIT top_10_ambiti 10;

ambiti_di_ricerca_e_numero_progetti = FOREACH limite_top_10_ambiti GENERATE Ambito_di_Ricerca AS Ambito_di_Ricerca, Numero_Progetti AS Numero_Progetti;

STORE ambiti_di_ricerca_e_numero_progetti INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Top10Ambiti.txt' USING PigStorage(',');
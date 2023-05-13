dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

istituzioni = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Research_Organization_original), ';')) AS Istituzione;

istituzioni_filtered = FILTER istituzioni BY Istituzione != 'University of Naples Federico II';

istituzioni_grouped = GROUP istituzioni_filtered BY Istituzione;
collaborazioni = FOREACH istituzioni_grouped GENERATE group AS Istituzione, COUNT(istituzioni_filtered) AS Numero_Collaborazioni;

istituzioni_ordered = ORDER collaborazioni BY Numero_Collaborazioni DESC;
top100istituzioni = LIMIT istituzioni_ordered 100;

STORE top100istituzioni INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Top100Istituzioni.txt' USING PigStorage(',');
dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

ricerche_cancro = FOREACH dataset_unina_research GENERATE Cancer_Types, Start_Year, Funding_Amount_in_EUR;

ricerche_cancro_filtrate = FILTER ricerche_cancro BY Cancer_Types != '' AND Start_Year > '2001';
fondi_cancro = GROUP ricerche_cancro_filtrate BY Start_Year;

fondi_stanziati = FOREACH fondi_cancro GENERATE group AS Anno, SUM(ricerche_cancro_filtrate.Funding_Amount_in_EUR) AS Somma_Finanziata_Cancro_EUR;
fondi_stanziati_ordinati = ORDER fondi_stanziati BY Anno DESC;

STORE fondi_stanziati_ordinati INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/FondiStanziatiCancroPerAnno.txt' USING PigStorage(',');
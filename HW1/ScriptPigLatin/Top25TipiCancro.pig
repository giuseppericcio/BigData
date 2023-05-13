dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);

tipi_cancro = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(TRIM(Cancer_Types), ';')) AS Tipo_Cancro;

tipi_cancro_filtrati = FILTER tipi_cancro BY Tipo_Cancro IS NOT NULL AND Tipo_Cancro != 'Not Site-Specific Cancer';

tipi_cancro_contati = GROUP tipi_cancro_filtrati BY Tipo_Cancro;
tipi_cancro_contati_final = FOREACH tipi_cancro_contati GENERATE group AS Tipo_Cancro, COUNT(tipi_cancro_filtrati) AS Numero_Progetti;

tipi_cancro_ordinati = ORDER tipi_cancro_contati_final BY Numero_Progetti DESC, Tipo_Cancro ASC;
tipi_cancro_finali = LIMIT tipi_cancro_ordinati 25;

STORE tipi_cancro_finali INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/Top25TipiCancro.txt' USING PigStorage(',');
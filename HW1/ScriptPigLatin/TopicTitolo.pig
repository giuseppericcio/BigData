dataset_unina_research = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/Dataset_Projects_Unina.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE') AS (Title_translated:chararray, Abstract_translated:chararray, Keywords:chararray, Funding_Amount_in_EUR:int, Start_Date:chararray, Start_Year:chararray, End_Date:chararray, End_Year:chararray, Researchers:chararray, Research_Organization_original:chararray, Research_Organization_standardized:chararray, City_of_Research_organization:chararray, Country_of_Research_organization:chararray, Funder:chararray, Funder_Group:chararray, Funder_Country:chararray, Program:chararray, Fields_of_Research_ANZSRC_2020:chararray, RCDC_Categories:chararray, HRCS_HC_Categories:chararray, HRCS_RAC_Categories:chararray, Cancer_Types:chararray, CSO_Categories:chararray, Units_of_Assessment:chararray, Sustainable_Development_Goals:chararray);
stoplist = LOAD '/mnt/c/Users/giuse/ScriptPigLatin/stopwords.txt' USING TextLoader AS (stop:CHARARRAY);

TopicTitolo = FOREACH dataset_unina_research GENERATE FLATTEN(TOKENIZE(REPLACE(LOWER(TRIM(Title_translated)),'[\\p{Punct},\\p{Cntrl}]',''))) AS Topic;
TopicTitolo_uniti = JOIN TopicTitolo BY Topic LEFT, stoplist BY stop;

TopicTitolo_puliti = FILTER TopicTitolo_uniti BY stoplist::stop IS NULL;

TopicTitolo_grp = GROUP TopicTitolo_puliti BY $0;

TopicTitolo_contati = FOREACH TopicTitolo_grp GENERATE $0, COUNT($1);

TopicTitolo_final = ORDER TopicTitolo_contati BY $1 DESC, $0 ASC;
Top100_Topic = LIMIT TopicTitolo_final 100;

STORE Top100_Topic INTO '/mnt/c/Users/giuse/ScriptPigLatin/Risultati/TopicTitolo.txt' USING PigStorage(',');
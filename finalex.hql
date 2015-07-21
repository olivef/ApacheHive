--Make sure that Tez is installed with a boostrap action

drop table IF EXISTS gdata;
create external table gdata(
EventId  DECIMAL,
Day  DECIMAL,
MonthYear  DECIMAL,
Year  DECIMAL,
FractionDate  DECIMAL,
Actor1Code  STRING,
Actor1Name  STRING,
Actor1CountryCode  STRING,
Actor1KnownGroupCode  STRING,
Actor1EthnicCode  STRING,
Actor1Religion1Code  STRING,
Actor1Religion2Code  STRING,
Actor1Type1Code  STRING,
Actor1Type2Code  STRING,
Actor1Type3Code  STRING,
Actor2Code  STRING,
Actor2Name  STRING,
Actor2CountryCode  STRING,
Actor2KnownGroupCode  STRING,
Actor2EthnicCode   STRING,
Actor2Religion1Code  STRING,
Actor2Religion2Code  STRING,
Actor2Type1Code  STRING,
Actor2Type2Code  STRING,
Actor2Type3Code  STRING,
IsRootEvent  DECIMAL,
EventCode  STRING,
EventBaseCode  STRING,
EventRootCode  STRING,
QuadClass  DECIMAL,
GoldsteinScale  DECIMAL,
NumMentions  DECIMAL,
NumSources  DECIMAL,
NumArticles  DECIMAL,
AvgTone  DECIMAL,
Actor1Geo_Type  DECIMAL,
Actor1Geo_FullName  STRING,
Actor1Geo_CountryCode  STRING,
Actor1Geo_ADM1Code  STRING,
Actor1Geo_Lat  DECIMAL,
Actor1Geo_Long  DECIMAL,
Actor1Geo_FeatureID  DECIMAL,
Actor2Geo_Type  DECIMAL,
Actor2Geo_FullName  STRING,
Actor2Geo_CountryCode  STRING,
Actor2Geo_ADM1Code  STRING,
Actor2Geo_Lat  DECIMAL,
Actor2Geo_Long  DECIMAL,
Actor2Geo_FeatureID  DECIMAL,
ActionGeo_Type  DECIMAL,
ActionGeo_FullName  STRING,
Country  STRING,
ActionGeo_ADM1Code  STRING,
ActionGeo_Lat  DECIMAL,
ActionGeo_Long  DECIMAL,
ActionGeo_FeatureID  DECIMAL,
DATEADDED  STRING,
SOURCEURL  STRING
 )
ROW FORMAT delimited fields terminated by  "\t"
stored as textfile location 's3://xxxxxx/gdeltfiles/';

drop table IF EXISTS  gdata_fim;
create external table gdata_fim(
Day  STRING,
Num_of_Events DECIMAL
 )
PARTITIONED BY (Country  STRING)
ROW FORMAT delimited fields terminated by  "\t" 
stored as textfile location 's3://xxxxxxxx/gdata_fim/';

SET REGEX="(\\d{4})(\\d{2})(\\d{2})";

set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=258;
set hive.exec.max.dynamic.partitions=258;
set hive.execution.engine=tez;
INSERT OVERWRITE TABLE gdata_fim 
PARTITION(Country)  Select 
CONCAT(
    regexp_extract( cast (Day as string), ${hiveconf:REGEX}, 1),'-',
    regexp_extract(cast (Day as string), ${hiveconf:REGEX}, 2),'-',
    regexp_extract(cast (Day as string), ${hiveconf:REGEX}, 3)
  ) as day,count(*),Country  
FROM gdata 
WHERE year in (2014,2015) and Country is not null 
GROUP BY Country,Day 
DISTRIBUTE BY Country, Day
SORT BY Country, Day desc;

--to do: apply UDF to format the date

show partitions gdata_fim;

- create database
create database excel;

use excel;
-- note: as in the HadoopOffice library .xls and .xlsx files are supported. You can also use all its options (encryption, signing, linked workbooks, templates, low footprint mode...)

-- create external table representing an Excel data stored in /user/office/files
-- we do not skip the header (ie the header will be returned as data)
-- specify a HadoopOffice option as an example (see here for all options: https://github.com/ZuInnoTe/hadoopoffice/wiki/Hadoop-File-Format)
-- based on example file: https://github.com/ZuInnoTe/hadoopoffice/blob/master/fileformat/src/test/resources/testsimple.xlsx?raw=true
-- all options: https://github.com/ZuInnoTe/hadoopoffice/wiki/Hive-Serde
--
create external table ExcelTable ROW FORMAT SERDE 'org.zuinnote.hadoop.excel.hive.serde.ExcelSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelFileInputFormat' OUTPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelRowFileOutputFormat' LOCATION '/user/office/files' TBLPROPERTIES("skip.header.line.count"="0", "hadoopoffice.read.locale.bcp47"="DE","hadoopoffice.write.locale.bcp47"="DE");

-- show schema (all columns are Text by default)
describe ExcelTable;

-- get number of rows
select count(*) from ExcelTable;

-- display the first 10 rows
select * from ExcelTable LIMIT 10;

-- simply insert by using select from any other table
create external table ExcelOut ROW FORMAT SERDE 'org.zuinnote.hadoop.excel.hive.serde.ExcelSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelFileInputFormat' OUTPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelRowFileOutputFormat' LOCATION '/user/office/files' TBLPROPERTIES("hadoopoffice.read.locale.bcp47"="DE","hadoopoffice.write.locale.bcp47"="DE")
AS 
select * from sourcetable;


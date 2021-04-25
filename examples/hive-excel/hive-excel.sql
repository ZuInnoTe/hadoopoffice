-- create database
create database excel;

use excel;
-- note: as in the HadoopOffice library .xls and .xlsx files are supported. You can also use all its options (encryption, signing, linked workbooks, templates, low footprint mode...)

-- create external table representing an Excel data stored in /user/office/files
-- we do skip the header line (assuming header is only in one sheet, for more fine-granular configuration, see https://github.com/ZuInnoTe/hadoopoffice/wiki/Hadoop-File-Format#header)
-- specify a HadoopOffice option as an example (see here for all options: https://github.com/ZuInnoTe/hadoopoffice/wiki/Hadoop-File-Format)
-- based on example file: https://github.com/ZuInnoTe/hadoopoffice/blob/master/fileformat/src/test/resources/testsimple.xlsx?raw=true
-- all options: https://github.com/ZuInnoTe/hadoopoffice/wiki/Hive-Serde

create external table ExcelTable(decimalsc1 decimal(3,2), booleancolumn boolean, datecolumn date, stringcolumn string, decimalp8sc3 decimal(8,3), bytecolumn tinyint, shortcolumn smallint, intcolumn int, longcolumn bigint) ROW FORMAT SERDE 'org.zuinnote.hadoop.excel.hive.serde.ExcelSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelFileInputFormat' OUTPUTFORMAT 'org.zuinnote.hadoop.excel.hive.outputformat.HiveExcelRowFileOutputFormat' LOCATION '/user/office/files' TBLPROPERTIES("hadoopoffice.read.simple.decimalformat"="DE","hadoopoffice.write.simple.dateformat"="DE","hadoopoffice.read.header.read"="true", "hadoopoffice.read.locale.bcp47"="DE","hadoopoffice.write.locale.bcp47"="DE");

-- show schema
describe ExcelTable;

-- get number of rows
select count(*) from ExcelTable;

-- display the first 10 rows
select * from ExcelTable LIMIT 10;


-- simply insert by using select from any other table
-- note: we instruct Hive to write the table in : /user/office/output
-- we use as decimalFormat locale "DE" which is Germany, because it fits to the file of the source table "ExcelTable" defined above
create  table ExcelOut ROW FORMAT SERDE 'org.zuinnote.hadoop.excel.hive.serde.ExcelSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelFileInputFormat' OUTPUTFORMAT 'org.zuinnote.hadoop.excel.hive.outputformat.HiveExcelRowFileOutputFormat' LOCATION '/user/office/output' TBLPROPERTIES("office.hive.write.defaultSheetName"="FirstSheet","hadoopoffice.read.header.read"="true","hadoopoffice.write.header.write"="true", "hadoopoffice.write.mimeType"="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "hadoopoffice.read.simple.decimalformat"="DE","hadoopoffice.write.simple.dateformat"="DE","hadoopoffice.read.locale.bcp47"="DE","hadoopoffice.write.locale.bcp47"="DE")
AS
select * from ExcelTable;

-- create database
create database excel;

use excel;
-- note: as in the HadoopOffice library .xls and .xlsx files are supported. You can also use all its options (encryption, signing, linked workbooks, templates, low footprint mode...)

-- create external table representing an Excel data stored in /user/office/files
-- note: contrary to the normal Excel Serde each row in this Serde describes one cell in Excel, ie the table has 5 columns: formattedValue, comment, formula, address, sheetName
(see: 
https://github.com/ZuInnoTe/hadoopoffice/blob/master/fileformat/src/main/java/org/zuinnote/hadoop/office/format/common/dao/SpreadSheetCellDAO.java)
-- based on example file: https://github.com/ZuInnoTe/hadoopoffice/blob/master/fileformat/src/test/resources/excel2013test.xlsx?raw=true
-- you can easily filter by any of the 5 characteristis of a cell
-- you do not need to specify the columns of the table, because this serde always has 5 columns describing a cell in Excel
-- all options: https://github.com/ZuInnoTe/hadoopoffice/wiki/Hive-Serde

create external table ExcelDAOTable ROW FORMAT SERDE 'org.zuinnote.hadoop.excel.hive.daoserde.ExcelSpreadSheetCellDAOSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelCellFileInputFormat' OUTPUTFORMAT 'org.zuinnote.hadoop.excel.hive.outputformat.HiveExcelCellFileOutputFormat' LOCATION '/user/office/files' TBLPROPERTIES("hadoopoffice.read.locale.bcp47"="DE","hadoopoffice.write.locale.bcp47"="DE");

-- show schema 
describe ExcelTable;

-- get number of rows
select count(*) from ExcelTable;

-- display the first 10 rows (ie cells)
select * from ExcelTable LIMIT 10;


-- store data as Excel
-- copies the cells from ExcelDAOTable. Note: it expects as source a table with 5 columns (formattedValue, comment, formula, address, sheetname) where each row corresponds to one Excel cell 
create  table ExcelDAOOut ROW FORMAT SERDE 'org.zuinnote.hadoop.excel.hive.daoserde.ExcelSpreadSheetCellDAOSerde' STORED AS INPUTFORMAT 'org.zuinnote.hadoop.office.format.mapred.ExcelCellFileInputFormat' OUTPUTFORMAT 'org.zuinnote.hadoop.excel.hive.outputformat.HiveExcelCellFileOutputFormat' LOCATION '/user/office/output' TBLPROPERTIES("hadoopoffice.write.mimeType"="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "hadoopoffice.read.locale.bcp47"="DE","hadoopoffice.write.locale.bcp47"="DE")
AS 
select * from ExcelDAOTable;
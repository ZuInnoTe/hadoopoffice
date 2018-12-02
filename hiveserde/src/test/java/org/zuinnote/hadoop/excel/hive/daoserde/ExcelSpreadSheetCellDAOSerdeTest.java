/**
* Copyright 2018 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/
package org.zuinnote.hadoop.excel.hive.daoserde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Locale;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.excel.hive.daoserde.ExcelSpreadSheetCellDAOSerde;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeReader;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;

/**
 * 
 *
 */
public class ExcelSpreadSheetCellDAOSerdeTest {

	@Test
	public void checkTestExcel2003SingleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003test.xls";
		String fileNameSpreadSheet = classLoader.getResource("testdata/" + fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013SingleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource("testdata/" + fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void initializePositive() throws SerDeException {
		ExcelSpreadSheetCellDAOSerde testSerde = new ExcelSpreadSheetCellDAOSerde();
		Configuration conf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty("hadoopoffice.write.header.write", "true");
		tblProperties.setProperty("hadoopoffice.read.locale.bcp47", "de");
		tblProperties.setProperty("hadoopoffice.read.linkedworkbooks", "true");
		tblProperties.setProperty(serdeConstants.LIST_COLUMNS, "column1,column2");
		tblProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string");
		testSerde.initialize(conf, tblProperties);
		assertEquals("de", conf.get("hadoopoffice.read.locale.bcp47", "us"),
				"HadoopOffice Hadoop configuration option set");
		assertTrue(conf.getBoolean("hadoopoffice.read.linkedworkbooks", false),
				"HaodoopOffice Hadoop configuration option set boolean");
	}

	@Test
	public void deserializeExcel2003SingleSheet() throws SerDeException, FileNotFoundException, FormatNotUnderstoodException {
		// initialize Serde
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003test.xls";
		String fileNameSpreadSheet = classLoader.getResource("testdata/" + fileName).getFile();
		ExcelSpreadSheetCellDAOSerde testSerde = new ExcelSpreadSheetCellDAOSerde();
		Configuration hadoopConf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty("hadoopoffice.read.locale.bcp47", "de");
		// serde automatically provides table column names and types
		testSerde.initialize(hadoopConf, tblProperties);
		// load data for testing

		FileInputStream documentInputStream = new FileInputStream(new File(fileNameSpreadSheet));

		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setMimeType("ms-excel");
		hocr.setLocale(Locale.GERMAN);
		OfficeReader reader = new OfficeReader(documentInputStream, hocr);
		reader.parse();
		// read first row
		Object[] row = reader.getNext();
		
		String[] cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("test1",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("test2",cellAr[0],"formatted Value correct");
		assertEquals("test", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("B1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[2]);
		assertEquals("test3",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("C1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[3]);
		assertEquals("test4",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("D1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read second row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("4",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A2",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read third row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("31/12/99",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A3",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("5",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("B3",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[4]);
		assertEquals("null",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("E3",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read fourth row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("1",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A4",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read fifth row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("2",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A5",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("6",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("A5*A6",cellAr[2],"formula correct");
		assertEquals("B5",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[2]);
		assertEquals("10",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("A2+B5",cellAr[2],"formula correct");
		assertEquals("C5",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read sixth row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("3",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A6",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("4",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("B6",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[2]);
		assertEquals("15",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("SUM(B3:B6)",cellAr[2],"formula correct");
		assertEquals("C6",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
	}
	
	
	@Test
	public void deserializeExcel2013SingleSheet() throws SerDeException, FileNotFoundException, FormatNotUnderstoodException {
		// initialize Serde
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource("testdata/" + fileName).getFile();
		ExcelSpreadSheetCellDAOSerde testSerde = new ExcelSpreadSheetCellDAOSerde();
		Configuration hadoopConf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty("hadoopoffice.read.locale.bcp47", "de");
		// serde automatically provides table column names and types
		testSerde.initialize(hadoopConf, tblProperties);
		// load data for testing

		FileInputStream documentInputStream = new FileInputStream(new File(fileNameSpreadSheet));

		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setMimeType("ms-excel");
		hocr.setLocale(Locale.GERMAN);
		OfficeReader reader = new OfficeReader(documentInputStream, hocr);
		reader.parse();
		// read first row
		Object[] row = reader.getNext();
		
		String[] cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("test1",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("test2",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("B1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[2]);
		assertEquals("test3",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("C1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[3]);
		assertEquals("test4",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("D1",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read second row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("4",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A2",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read third row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("31/12/99",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A3",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("5",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("B3",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[4]);
		assertEquals("null",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("E3",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read fourth row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("1",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A4",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read fifth row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("2",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A5",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("6",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("A5*A6",cellAr[2],"formula correct");
		assertEquals("B5",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[2]);
		assertEquals("10",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("A2+B5",cellAr[2],"formula correct");
		assertEquals("C5",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		// read sixth row
		row = reader.getNext();
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[0]);
		assertEquals("3",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("A6",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[1]);
		assertEquals("4",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("",cellAr[2],"formula correct");
		assertEquals("B6",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
		cellAr = (String[]) testSerde.deserialize((SpreadSheetCellDAO)row[2]);
		assertEquals("15",cellAr[0],"formatted Value correct");
		assertEquals("", cellAr[1], "comment correct");
		assertEquals("SUM(B3:B6)",cellAr[2],"formula correct");
		assertEquals("C6",cellAr[3],"address correct");
		assertEquals("Sheet1",cellAr[4],"sheetname correct");
	}
	
	@Test
	public void serialize() throws SerDeException {
		// initialize Serde
		ExcelSpreadSheetCellDAOSerde testSerde = new ExcelSpreadSheetCellDAOSerde();
		Configuration hadoopConf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty("hadoopoffice.write.locale.bcp47", "de");
		testSerde.initialize(hadoopConf, tblProperties);
		// get object inspector
		ObjectInspector oi = testSerde.getObjectInspector();
		String[] testHiveStructure = new String[5];
		testHiveStructure[0] = "test1";
		testHiveStructure[1] = "no comment";
		testHiveStructure[2] = "A1*A2";
		testHiveStructure[3] = "A3";
		testHiveStructure[4] = "Sheet1";
		SpreadSheetCellDAO resultDAO = (SpreadSheetCellDAO) testSerde.serialize(testHiveStructure, oi);
		assertEquals(testHiveStructure[0],resultDAO.getFormattedValue(),"formatted value correct");
		assertEquals(testHiveStructure[1],resultDAO.getComment(),"comment correct");
		assertEquals(testHiveStructure[2],resultDAO.getFormula(), "formula correct");
		assertEquals(testHiveStructure[3],resultDAO.getAddress(), "address correct");
		assertEquals(testHiveStructure[4],resultDAO.getSheetName(),"sheetname correct");
	}
}

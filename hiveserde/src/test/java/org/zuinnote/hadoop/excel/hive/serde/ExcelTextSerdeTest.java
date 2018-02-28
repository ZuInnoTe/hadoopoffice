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
package org.zuinnote.hadoop.excel.hive.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeReader;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAOArrayWritable;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;

/**
 *
 *
 */
public class ExcelTextSerdeTest {

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
	public void checkTestExcel2013SimpleAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "testsimple.xlsx";
		String fileNameSpreadSheet = classLoader.getResource("testdata/" + fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void initializePositive() throws SerDeException {
		ExcelSerde testSerde = new ExcelSerde();
		Configuration conf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty(ExcelSerde.CONF_DATEFORMAT, "de");
		tblProperties.setProperty(ExcelSerde.CONF_DECIMALFORMAT, "de");
		tblProperties.setProperty(ExcelSerde.CONF_DEFAULTSHEETNAME, "Sheet2");
		tblProperties.setProperty(ExcelSerde.CONF_WRITEHEADER, "true");
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
	public void deserializeSimpleExcel2013()
			throws IOException, FormatNotUnderstoodException, SerDeException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "testsimple.xlsx";
		String fileNameSpreadSheet = classLoader.getResource("testdata/" + fileName).getFile();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // used only for assertions
		ExcelSerde testSerde = new ExcelSerde();
		Configuration hadoopConf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty(ExcelSerde.CONF_DATEFORMAT, "us");
		tblProperties.setProperty(ExcelSerde.CONF_DECIMALFORMAT, "de");
		tblProperties.setProperty(ExcelSerde.CONF_DEFAULTSHEETNAME, "Sheet1");
		tblProperties.setProperty(ExcelSerde.CONF_WRITEHEADER, "false");
		tblProperties.setProperty("hadoopoffice.read.locale.bcp47", "de");
		tblProperties.setProperty(serdeConstants.LIST_COLUMNS,
				"decimalsc1,booleancolumn,datecolumn,stringcolumn,decimalp8sc3,bytecolumn,shortcolumn,intcolumn,longcolumn");
		tblProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES,
				"decimal(3,2),boolean,date,string,decimal(8,3),tinyint,smallint,int,bigint");
		testSerde.initialize(hadoopConf, tblProperties);

		FileInputStream documentInputStream = new FileInputStream(new File(fileNameSpreadSheet));

		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setMimeType("ms-excel");
		OfficeReader reader = new OfficeReader(documentInputStream, hocr);
		reader.parse();
		// skip header
		Object[] header = reader.getNext();
		assertNotNull(header, "Header is existing");
		SpreadSheetCellDAOArrayWritable usableObject = new SpreadSheetCellDAOArrayWritable();
		
		SpreadSheetCellDAO[] row1 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row1);
		Object[] simpleRow1 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("1.00")), simpleRow1[0], "A2 = 1.00");
		assertTrue((Boolean) simpleRow1[1], "B2 = TRUE");
		assertEquals(sdf.parse("2017-01-01"), simpleRow1[2], "C2 = 2017-01-01");
		assertEquals("This is a text", simpleRow1[3], "D2 = This is a text");
		assertEquals(HiveDecimal.create(new BigDecimal("10.000")), simpleRow1[4], "E2 = 10.000");
		assertEquals((byte) 3, simpleRow1[5], "F2 = 3");
		assertEquals((short) 3, simpleRow1[6], "G2 = 3");
		assertEquals((int) 100, simpleRow1[7], "H2 = 100");
		assertEquals(65335L, simpleRow1[8], "I2 = 65335");

		SpreadSheetCellDAO[] row2 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row2);
		Object[] simpleRow2 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("1.50")), simpleRow2[0], "A3 = 1.50");
		assertFalse((Boolean) simpleRow2[1], "B3 = FALSE");
		assertEquals(sdf.parse("2017-02-28"), simpleRow2[2], "C3 = 2017-02-28");
		assertEquals("Another String", simpleRow2[3], "D3 = Another String");
		assertEquals(HiveDecimal.create(new BigDecimal("2.334")), simpleRow2[4], "E3 = 2.334");
		assertEquals((byte) 5, simpleRow2[5], "F3 = 5");
		assertEquals((short) 4, simpleRow2[6], "G3 = 4");
		assertEquals((int) 65335, simpleRow2[7], "H3 = 65335");
		assertEquals(1L, simpleRow2[8], "I3 = 1");

		SpreadSheetCellDAO[] row3 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row3);
		Object[] simpleRow3 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("3.40")), simpleRow3[0], "A4 = 3.40");
		assertFalse((Boolean) simpleRow3[1], "B4 = FALSE");
		assertEquals(sdf.parse("2000-02-29"), simpleRow3[2], "C4 = 2000-02-29");
		assertEquals("10", simpleRow3[3], "D4 = 10");
		assertEquals(HiveDecimal.create(new BigDecimal("4.500")), simpleRow3[4], "E4 = 4.500");
		assertEquals((byte) -100, simpleRow3[5], "F4 = -100");
		assertEquals((short) 5, simpleRow3[6], "G4 = 5");
		assertEquals((int) 1, simpleRow3[7], "H4 = 1");
		assertEquals(250L, simpleRow3[8], "I4 = 250");

		SpreadSheetCellDAO[] row4 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row4);
		Object[] simpleRow4 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("5.50")), simpleRow4[0], "A5 = 5.50");
		assertFalse((Boolean) simpleRow4[1], "B5 = FALSE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow4[2], "C5 = 2017-03-01");
		assertEquals("test3", simpleRow4[3], "D5 = test3");
		assertEquals(HiveDecimal.create(new BigDecimal("11.000")), simpleRow4[4], "E5 = 11.000");
		assertEquals((byte) 2, simpleRow4[5], "F5 = 2");
		assertEquals((short) 250, simpleRow4[6], "G5 = 250");
		assertEquals((int) 250, simpleRow4[7], "H5 = 250");
		assertEquals(10L, simpleRow4[8], "I5 = 10");

		SpreadSheetCellDAO[] row5 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row5);
		Object[] simpleRow5 = (Object[]) testSerde.deserialize(usableObject);
		assertNull(simpleRow5[0], "A6 = null");
		assertNull(simpleRow5[1], "B6 = null");
		assertNull(simpleRow5[2], "C6 = null");
		assertEquals("test4", simpleRow5[3], "D6 = test4");
		assertEquals(HiveDecimal.create(new BigDecimal("100.000")), simpleRow5[4], "E6 = 100.000");
		assertEquals((byte) 3, simpleRow5[5], "F6 = 3");
		assertEquals((short) 3, simpleRow5[6], "G6 = 3");
		assertEquals((int) 5, simpleRow5[7], "H6 = 5");
		assertEquals(3147483647L, simpleRow5[8], "I6 = 3147483647");

		SpreadSheetCellDAO[] row6 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row6);
		Object[] simpleRow6 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("3.40")), simpleRow6[0], "A7 = 3.40");
		assertTrue((Boolean) simpleRow6[1], "B7 = TRUE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow6[2], "C7 = 2017-03-01");
		assertEquals("test5", simpleRow6[3], "D7 = test5");
		assertEquals(HiveDecimal.create(new BigDecimal("10000.500")), simpleRow6[4], "E6 = 10000.500");
		assertEquals((byte) 120, simpleRow6[5], "F7 = 120");
		assertEquals((short) 100, simpleRow6[6], "G7 = 100");
		assertEquals((int) 10000, simpleRow6[7], "H7 = 10000");
		assertEquals(10L, simpleRow6[8], "I6 = 10");
		if (reader != null) {
			reader.close();
		}
		if (documentInputStream != null) {
			documentInputStream.close();
		}
	}

	@Test
	public void serializeSimple2013WithoutHeader()
			throws SerDeException, FormatNotUnderstoodException, ParseException, IOException {

		// initialize Serde
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "testsimple.xlsx";
		String fileNameSpreadSheet = classLoader.getResource("testdata/" + fileName).getFile();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // used only for assertions
		ExcelSerde testSerde = new ExcelSerde();
		Configuration hadoopConf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty(ExcelSerde.CONF_DATEFORMAT, "us");
		tblProperties.setProperty(ExcelSerde.CONF_DECIMALFORMAT, "de");
		tblProperties.setProperty(ExcelSerde.CONF_DEFAULTSHEETNAME, "Sheet1");
		tblProperties.setProperty(ExcelSerde.CONF_WRITEHEADER, "false");
		tblProperties.setProperty("hadoopoffice.read.locale.bcp47", "de");
		tblProperties.setProperty(serdeConstants.LIST_COLUMNS,
				"decimalsc1,booleancolumn,datecolumn,stringcolumn,decimalp8sc3,bytecolumn,shortcolumn,intcolumn,longcolumn");
		tblProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES,
				"decimal(3,2),boolean,date,string,decimal(8,3),tinyint,smallint,int,bigint");
		testSerde.initialize(hadoopConf, tblProperties);

		// 1) prepare data (=deserialize)

		FileInputStream documentInputStream = new FileInputStream(new File(fileNameSpreadSheet));

		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setMimeType("ms-excel");
		OfficeReader reader = new OfficeReader(documentInputStream, hocr);
		reader.parse();
		// skip header
		Object[] header = reader.getNext();
		assertNotNull(header, "Header is existing");
		// start reading first row
		SpreadSheetCellDAOArrayWritable usableObject = new SpreadSheetCellDAOArrayWritable();
		SpreadSheetCellDAO[] row1 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row1);
		Object[] simpleRow1 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("1.00")), simpleRow1[0], "A2 = 1.00");
		assertTrue((Boolean) simpleRow1[1], "B2 = TRUE");
		assertEquals(sdf.parse("2017-01-01"), simpleRow1[2], "C2 = 2017-01-01");
		assertEquals("This is a text", simpleRow1[3], "D2 = This is a text");
		assertEquals(HiveDecimal.create(new BigDecimal("10.000")), simpleRow1[4], "E2 = 10.000");
		assertEquals((byte) 3, simpleRow1[5], "F2 = 3");
		assertEquals((short) 3, simpleRow1[6], "G2 = 3");
		assertEquals((int) 100, simpleRow1[7], "H2 = 100");
		assertEquals(65335L, simpleRow1[8], "I2 = 65335");

		SpreadSheetCellDAO[] row2 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row2);
		Object[] simpleRow2 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("1.50")), simpleRow2[0], "A3 = 1.50");
		assertFalse((Boolean) simpleRow2[1], "B3 = FALSE");
		assertEquals(sdf.parse("2017-02-28"), simpleRow2[2], "C3 = 2017-02-28");
		assertEquals("Another String", simpleRow2[3], "D3 = Another String");
		assertEquals(HiveDecimal.create(new BigDecimal("2.334")), simpleRow2[4], "E3 = 2.334");
		assertEquals((byte) 5, simpleRow2[5], "F3 = 5");
		assertEquals((short) 4, simpleRow2[6], "G3 = 4");
		assertEquals((int) 65335, simpleRow2[7], "H3 = 65335");
		assertEquals(1L, simpleRow2[8], "I3 = 1");

		SpreadSheetCellDAO[] row3 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row3);
		Object[] simpleRow3 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("3.40")), simpleRow3[0], "A4 = 3.40");
		assertFalse((Boolean) simpleRow3[1], "B4 = FALSE");
		assertEquals(sdf.parse("2000-02-29"), simpleRow3[2], "C4 = 2000-02-29");
		assertEquals("10", simpleRow3[3], "D4 = 10");
		assertEquals(HiveDecimal.create(new BigDecimal("4.500")), simpleRow3[4], "E4 = 4.500");
		assertEquals((byte) -100, simpleRow3[5], "F4 = -100");
		assertEquals((short) 5, simpleRow3[6], "G4 = 5");
		assertEquals((int) 1, simpleRow3[7], "H4 = 1");
		assertEquals(250L, simpleRow3[8], "I4 = 250");

		SpreadSheetCellDAO[] row4 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row4);
		Object[] simpleRow4 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("5.50")), simpleRow4[0], "A5 = 5.50");
		assertFalse((Boolean) simpleRow4[1], "B5 = FALSE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow4[2], "C5 = 2017-03-01");
		assertEquals("test3", simpleRow4[3], "D5 = test3");
		assertEquals(HiveDecimal.create(new BigDecimal("11.000")), simpleRow4[4], "E5 = 11.000");
		assertEquals((byte) 2, simpleRow4[5], "F5 = 2");
		assertEquals((short) 250, simpleRow4[6], "G5 = 250");
		assertEquals((int) 250, simpleRow4[7], "H5 = 250");
		assertEquals(10L, simpleRow4[8], "I5 = 10");

		SpreadSheetCellDAO[] row5 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row5);
		Object[] simpleRow5 = (Object[]) testSerde.deserialize(usableObject);
		assertNull(simpleRow5[0], "A6 = null");
		assertNull(simpleRow5[1], "B6 = null");
		assertNull(simpleRow5[2], "C6 = null");
		assertEquals("test4", simpleRow5[3], "D6 = test4");
		assertEquals(HiveDecimal.create(new BigDecimal("100.000")), simpleRow5[4], "E6 = 100.000");
		assertEquals((byte) 3, simpleRow5[5], "F6 = 3");
		assertEquals((short) 3, simpleRow5[6], "G6 = 3");
		assertEquals((int) 5, simpleRow5[7], "H6 = 5");
		assertEquals(3147483647L, simpleRow5[8], "I6 = 3147483647");

		SpreadSheetCellDAO[] row6 = (SpreadSheetCellDAO[]) reader.getNext();
		usableObject.set(row6);
		Object[] simpleRow6 = (Object[]) testSerde.deserialize(usableObject);
		assertEquals(HiveDecimal.create(new BigDecimal("3.40")), simpleRow6[0], "A7 = 3.40");
		assertTrue((Boolean) simpleRow6[1], "B7 = TRUE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow6[2], "C7 = 2017-03-01");
		assertEquals("test5", simpleRow6[3], "D7 = test5");
		assertEquals(HiveDecimal.create(new BigDecimal("10000.500")), simpleRow6[4], "E6 = 10000.500");
		assertEquals((byte) 120, simpleRow6[5], "F7 = 120");
		assertEquals((short) 100, simpleRow6[6], "G7 = 100");
		assertEquals((int) 10000, simpleRow6[7], "H7 = 10000");
		assertEquals(10L, simpleRow6[8], "I6 = 10");
	
		if (reader != null) {
			reader.close();
		}
		if (documentInputStream != null) {
			documentInputStream.close();
		}
		// 2) serialize
		 // get object inspector
		ObjectInspector oi = testSerde.getObjectInspector();
		SpreadSheetCellDAOArrayWritable row1SSCDW = (SpreadSheetCellDAOArrayWritable)testSerde.serialize(simpleRow1, oi);
		SpreadSheetCellDAO[] row1SSCD = (SpreadSheetCellDAO[])row1SSCDW.get();
		// note that Hive strips of trailing zeros....
		assertEquals("1.00",row1SSCD[0].getFormula(),"A2 = 1");
		SpreadSheetCellDAOArrayWritable row2SSCDW = (SpreadSheetCellDAOArrayWritable)testSerde.serialize(simpleRow2, oi);
		SpreadSheetCellDAOArrayWritable row3SSCDW = (SpreadSheetCellDAOArrayWritable)testSerde.serialize(simpleRow3, oi);
		SpreadSheetCellDAOArrayWritable row4SSCDW = (SpreadSheetCellDAOArrayWritable)testSerde.serialize(simpleRow4, oi);
		SpreadSheetCellDAOArrayWritable row5SSCDW = (SpreadSheetCellDAOArrayWritable)testSerde.serialize(simpleRow5, oi);
		SpreadSheetCellDAOArrayWritable row6SSCDW = (SpreadSheetCellDAOArrayWritable)testSerde.serialize(simpleRow6, oi);
	}

}

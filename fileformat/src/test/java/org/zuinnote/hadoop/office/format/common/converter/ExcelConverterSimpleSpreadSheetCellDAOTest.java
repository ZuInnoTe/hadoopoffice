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
package org.zuinnote.hadoop.office.format.common.converter;

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
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Locale;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mortbay.log.Log;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeReader;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBigDecimalDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBooleanDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericByteDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDateDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericIntegerDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericLongDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericShortDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericStringDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericTimestampDataType;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;

/**
 * @author jornfranke
 *
 */
public class ExcelConverterSimpleSpreadSheetCellDAOTest {

	   @BeforeAll
	    public static void oneTimeSetUp() throws IOException {
	      // one-time initialization code   

	    }

	    @AfterAll
	    public static void oneTimeTearDown() {
	        // one-time cleanup code
	      }

	    @BeforeEach
	    public void setUp() {
	    }

	    @AfterEach
	    public void tearDown() {  
	    }
	    
	    @Test
	    public void checkTestExcelSimpleSheetAvailable() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="testsimple.xlsx";
			String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
			assertNotNull(fileNameSpreadSheet,"Test Data File \""+fileName+"\" is not null in resource path");
			File file = new File(fileNameSpreadSheet);
			assertTrue(file.exists(),"Test Data File \""+fileName+"\" exists");
			assertFalse(file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	    }
	    

	    @Test
	    public void convertCaseTestSimple() throws FileNotFoundException, ParseException, FormatNotUnderstoodException {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="testsimple.xlsx";
			String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    			File file = new File(fileNameSpreadSheet);
	    		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
	    		hocr.setFileName(fileName);
	    		hocr.setMimeType("ms-excel");
	    		hocr.setLocale(Locale.GERMAN);
	    		OfficeReader officeReader = new OfficeReader(new FileInputStream(file), hocr);  
	    		officeReader.parse();
	    		// read excel file
	    		ArrayList<SpreadSheetCellDAO[]> excelContent = new ArrayList<>();
	    		SpreadSheetCellDAO[] currentRow =(SpreadSheetCellDAO[]) officeReader.getNext();
	   
	    		while (currentRow!=null) {
	    			excelContent.add(currentRow);
	    			currentRow = (SpreadSheetCellDAO[]) officeReader.getNext();
	    		
	    		}
	    		
	    		assertEquals(7,excelContent.size(),"7 rows (including header) read");
	    		// configure converter
	    		SimpleDateFormat dateFormat = (SimpleDateFormat)DateFormat.getDateInstance(DateFormat.SHORT, Locale.US);
	    		DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(Locale.GERMAN);
	    		ExcelConverterSimpleSpreadSheetCellDAO converter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat,decimalFormat);
	    		///// auto detect schema (skip header)
	    		for (int i=1;i<excelContent.size();i++) {
	    			converter.updateSpreadSheetCellRowToInferSchemaInformation(excelContent.get(i));
	    		}
	    		GenericDataType[] schema = converter.getSchemaRow();
	    		// check schema
	    		assertTrue(schema[0] instanceof GenericBigDecimalDataType, "First column is a decimal");
	    		assertEquals(2,((GenericBigDecimalDataType)schema[0]).getPrecision(), "First column decimal has precision 2");
	    		assertEquals(1,((GenericBigDecimalDataType)schema[0]).getScale(), "First column decimal has scale 1");
	    		assertTrue(schema[1] instanceof GenericBooleanDataType, "Second column is a boolean");
	    		assertTrue(schema[2] instanceof GenericDateDataType, "Third column is a date");
	    		assertTrue(schema[3] instanceof GenericStringDataType, "Fourth column is a String");
	    		assertTrue(schema[4] instanceof GenericBigDecimalDataType, "Fifth column is a decimal");
	    		assertEquals(8,((GenericBigDecimalDataType)schema[4]).getPrecision(), "Fifth column decimal has precision 8");
	    		assertEquals(3,((GenericBigDecimalDataType)schema[4]).getScale(), "Fifth column decimal has scale 3");
	    		assertTrue(schema[5] instanceof GenericByteDataType, "Sixth column is a byte");
	    		assertTrue(schema[6] instanceof GenericShortDataType, "Seventh column is a short");
	    		assertTrue(schema[7] instanceof GenericIntegerDataType, "Eighth column is an integer");
	    		assertTrue(schema[8] instanceof GenericLongDataType, "Ninth column is a long");
	    		///// check conversion
	    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    		// check data (skip row 0, because it contains header)
	    		Object[] simpleRow1 = converter.getDataAccordingToSchema(excelContent.get(1));
	    		assertEquals(new BigDecimal("1.00"),simpleRow1[0], "A2 = 1.00");
	    		assertTrue((Boolean)simpleRow1[1],"B2 = TRUE");
	    		assertEquals(sdf.parse("2017-01-01"),simpleRow1[2],"C2 = 2017-01-01");
	    		assertEquals("This is a text",simpleRow1[3], "D2 = This is a text");
	    		assertEquals(new BigDecimal("10.000"),simpleRow1[4], "E2 = 10.000");
	    		assertEquals((byte)3,simpleRow1[5], "F2 = 3");
	    		assertEquals((short)3,simpleRow1[6], "G2 = 3");
	    		assertEquals((int)100,simpleRow1[7], "H2 = 100");
	    		assertEquals(65335L,simpleRow1[8], "I2 = 65335");
	    		Object[] simpleRow2 = converter.getDataAccordingToSchema(excelContent.get(2));
	    		assertEquals(new BigDecimal("1.50"),simpleRow2[0], "A3 = 1.50");
	    		assertFalse((Boolean)simpleRow2[1],"B3 = FALSE");
	    		assertEquals(sdf.parse("2017-02-28"),simpleRow2[2],"C3 = 2017-02-28");
	    		assertEquals("Another String",simpleRow2[3], "D3 = Another String");
	    		assertEquals(new BigDecimal("2.334"),simpleRow2[4], "E3 = 2.334");
	    		assertEquals((byte)5,simpleRow2[5], "F3 = 5");
	    		assertEquals((short)4,simpleRow2[6], "G3 = 4");
	    		assertEquals((int)65335,simpleRow2[7], "H3 = 65335");
	    		assertEquals(1L,simpleRow2[8], "I3 = 1");
	    		Object[] simpleRow3 = converter.getDataAccordingToSchema(excelContent.get(3));
	    		assertEquals(new BigDecimal("3.40"),simpleRow3[0], "A4 = 3.40");
	    		assertFalse((Boolean)simpleRow3[1],"B4 = FALSE");
	    		assertEquals(sdf.parse("2000-02-29"),simpleRow3[2],"C4 = 2000-02-29");
	    		assertEquals("10",simpleRow3[3], "D4 = 10");
	    		assertEquals(new BigDecimal("4.500"),simpleRow3[4], "E4 = 4.500");
	    		assertEquals((byte)-100,simpleRow3[5], "F4 = -100");
	    		assertEquals((short)5,simpleRow3[6], "G4 = 5");
	    		assertEquals((int)1,simpleRow3[7], "H4 = 1");
	    		assertEquals(250L,simpleRow3[8], "I4 = 250");
	    		Object[] simpleRow4 = converter.getDataAccordingToSchema(excelContent.get(4));
	    		assertEquals(new BigDecimal("5.50"),simpleRow4[0], "A5 = 5.50");
	    		assertFalse((Boolean)simpleRow4[1],"B5 = FALSE");
	    		assertEquals(sdf.parse("2017-03-01"),simpleRow4[2],"C5 = 2017-03-01");
	    		assertEquals("test3",simpleRow4[3], "D5 = test3");
	    		assertEquals(new BigDecimal("11.000"),simpleRow4[4], "E5 = 11.000");
	    		assertEquals((byte)2,simpleRow4[5], "F5 = 2");
	    		assertEquals((short)250,simpleRow4[6], "G5 = 250");
	    		assertEquals((int)250,simpleRow4[7], "H5 = 250");
	    		assertEquals(10L,simpleRow4[8], "I5 = 10");
	    		Object[] simpleRow5 = converter.getDataAccordingToSchema(excelContent.get(5));
	    		assertNull(simpleRow5[0], "A6 = null");
	    		assertNull(simpleRow5[1], "B6 = null");
	    		assertNull(simpleRow5[2], "C6 = null");
	    		assertEquals("test4",simpleRow5[3], "D6 = test4");
	    		assertEquals(new BigDecimal("100.000"),simpleRow5[4], "E6 = 100.000");
	    		assertEquals((byte)3,simpleRow5[5], "F6 = 3");
	    		assertEquals((short)3,simpleRow5[6], "G6 = 3");
	    		assertEquals((int)5,simpleRow5[7], "H6 = 5");
	    		assertEquals(3147483647L,simpleRow5[8], "I6 = 3147483647");
	    		Object[] simpleRow6 = converter.getDataAccordingToSchema(excelContent.get(6));
	    		assertEquals(new BigDecimal("3.40"),simpleRow6[0], "A7 = 3.40");
	    		assertTrue((Boolean)simpleRow6[1],"B7 = TRUE");
	    		assertEquals(sdf.parse("2017-03-01"),simpleRow6[2],"C7 = 2017-03-01");
	    		assertEquals("test5",simpleRow6[3], "D7 = test5");
	    		assertEquals(new BigDecimal("10000.500"),simpleRow6[4], "E6 = 10000.500");
	    		assertEquals((byte)120,simpleRow6[5], "F7 = 120");
	    		assertEquals((short)100,simpleRow6[6], "G7 = 100");
	    		assertEquals((int)10000,simpleRow6[7], "H7 = 10000");
	    		assertEquals(10L,simpleRow6[8], "I6 = 10");
	    }
	    
	    
	    @Test
	    public void convertCaseTestDateTimeStamp() throws FileNotFoundException, FormatNotUnderstoodException, ParseException {
	    	ClassLoader classLoader = getClass().getClassLoader();
			String fileName="datetimestamp.xlsx";
			String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    			File file = new File(fileNameSpreadSheet);
	    		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
	    		hocr.setFileName(fileName);
	    		hocr.setMimeType("ms-excel");
	    		hocr.setLocale(Locale.GERMAN);
	    		OfficeReader officeReader = new OfficeReader(new FileInputStream(file), hocr);  
	    		officeReader.parse();
	    		// read excel file
	    		ArrayList<SpreadSheetCellDAO[]> excelContent = new ArrayList<>();
	    		SpreadSheetCellDAO[] currentRow =(SpreadSheetCellDAO[]) officeReader.getNext();
	   
	    		while (currentRow!=null) {
	    			excelContent.add(currentRow);
	    			currentRow = (SpreadSheetCellDAO[]) officeReader.getNext();
	    		
	    		}
	    		
	    		assertEquals(5,excelContent.size(),"5 rows (including header) read");
	    		// configure converter
	    		SimpleDateFormat dateFormat = (SimpleDateFormat)DateFormat.getDateInstance(DateFormat.SHORT, Locale.US);
	    		SimpleDateFormat timeStampFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	    		
	    		DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(Locale.GERMAN);
	    		ExcelConverterSimpleSpreadSheetCellDAO converter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat,decimalFormat,timeStampFormat);
	    		///// auto detect schema (skip header)
	    		for (int i=1;i<excelContent.size();i++) {
	    			converter.updateSpreadSheetCellRowToInferSchemaInformation(excelContent.get(i));
	    		}
	    		GenericDataType[] schema = converter.getSchemaRow();
	    		// check schema
	    		
	    		assertEquals(3,schema.length,"File contains three columns");
	    		assertTrue(schema[0] instanceof GenericDateDataType, "First column is a date");
	    		//assertFalse(((GenericDateDataType)schema[0]).getIncludeTime(),"First column does not contain time data");
	    		assertTrue(schema[1] instanceof GenericTimestampDataType, "Second column is a timestamp");
	    		//assertFalse(((GenericDateDataType)schema[1]).getIncludeTime(),"Second column contains time data");
	    		assertTrue(schema[2] instanceof GenericTimestampDataType, "Third column is a timestamp");
	    		//assertFalse(((GenericDateDataType)schema[2]).getIncludeTime(),"Third column contains time data");
	    		// first row skip header beforehand
	    		///// check conversion
	    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    		SimpleDateFormat sdfTime =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    		// check data (skip row 0, because it contains header)
	    
	    		Object[] simpleRow1 = converter.getDataAccordingToSchema(excelContent.get(1));
	    		assertEquals(sdf.parse("2018-08-07"),simpleRow1[0],"A2 = 2018-08-07");
	    		assertEquals(sdfTime.parse("2018-08-07 12:00:00"),simpleRow1[1],"B2 = 2018-08-07 12:00:00");
	    		assertEquals(Timestamp.valueOf("2018-08-08 12:00:00.001"),simpleRow1[2],"C2 = 2018-08-08 12:00:00.001");
	    		Object[] simpleRow2 = converter.getDataAccordingToSchema(excelContent.get(2));
	    		assertEquals(sdf.parse("1999-03-03"),simpleRow2[0],"A3 = 1999-03-03");
	    		assertEquals(sdfTime.parse("1999-03-03 13:00:00"),simpleRow2[1],"B3 = 1999-03-03 13:00:00");
	    		assertEquals(Timestamp.valueOf("2006-02-26 13:01:01.002"),simpleRow2[2],"C3 = 2006-02-26 13:01:01.002");

	    		Object[] simpleRow3 = converter.getDataAccordingToSchema(excelContent.get(3));
	    		assertEquals(sdf.parse("2000-02-29"),simpleRow3[0],"A4 = 2000-02-29");
	    		assertEquals(sdfTime.parse("2000-02-29 13:01:01"),simpleRow3[1],"B4 = 2000-02-29 13:01:01");
	    		assertEquals(Timestamp.valueOf("2000-02-29 13:01:01.002"),simpleRow3[2],"C4 = 2000-02-29 13:01:01.002");

	    		Object[] simpleRow4 = converter.getDataAccordingToSchema(excelContent.get(4));
	    		assertEquals(sdf.parse("1999-12-31"),simpleRow4[0],"A5 = 1999-12-31");
	    		assertEquals(sdfTime.parse("1999-12-31 00:00:00"),simpleRow4[1],"B5 = 1999-12-31 00:00:00");
	    		assertEquals(Timestamp.valueOf("1999-12-31 23:59:59.003"),simpleRow4[2],"C4 = 1999-12-31 23:59:59.003");
	    		
	    	
	    }
	    
	    
	    @Test
	    public void getSpreadSheetCellDAOfromSimpleDataType() throws ParseException {
		    	// configure converter
	    		SimpleDateFormat dateFormat = (SimpleDateFormat)DateFormat.getDateInstance(DateFormat.SHORT, Locale.US);
	    		DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(Locale.GERMAN);
	    		ExcelConverterSimpleSpreadSheetCellDAO converter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat,decimalFormat);
	    		// test some rows
	    		Object[] rowA = new Object[13];
	    		rowA[0] = new Boolean(true);
	    		rowA[1] = new Byte((byte) 100);
	    		rowA[2] = new Short((short) 1000);
	    		rowA[3] = new Integer(32000);
	    		rowA[4] = new Long(65536L);
	    		rowA[5] = new Double(2.5);
	    		rowA[6] = new Float(3.5f);
	    		rowA[7] = new BigDecimal(1.5);
	    		rowA[8] = dateFormat.parse("1/13/2018");
	    		rowA[9] = new java.sql.Date(dateFormat.parse("1/13/2018").getTime());
	    		rowA[10] = new Timestamp(dateFormat.parse("1/13/2018").getTime());
	    		rowA[11] = "This is a test";
	    		rowA[12] = null;
	    		SpreadSheetCellDAO[] actual = converter.getSpreadSheetCellDAOfromSimpleDataType(rowA, "testsheet", 0);
	    		assertEquals("TRUE",actual[0].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[0].getComment(),"Comment is empty");
	    		assertEquals("",actual[0].getFormula(),"Formula contains data type");
	    		assertEquals("A1",actual[0].getAddress(),"Address is correct");
	    		assertEquals("100",actual[1].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[1].getComment(),"Comment is empty");
	    		assertEquals("",actual[1].getFormula(),"Formula contains data type");
	    		assertEquals("B1",actual[1].getAddress(),"Address is correct");
	    		assertEquals("1000",actual[2].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[2].getComment(),"Comment is empty");
	    		assertEquals("",actual[2].getFormula(),"Formula contains data type");
	    		assertEquals("C1",actual[2].getAddress(),"Address is correct");
	    		assertEquals("32000",actual[3].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[3].getComment(),"Comment is empty");
	    		assertEquals("",actual[3].getFormula(),"Formula contains data type");
	    		assertEquals("D1",actual[3].getAddress(),"Address is correct");
	    		assertEquals("65536",actual[4].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[4].getComment(),"Comment is empty");
	    		assertEquals("",actual[4].getFormula(),"Formula contains data type");
	    		assertEquals("E1",actual[4].getAddress(),"Address is correct");
	    		assertEquals("2.5",actual[5].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[5].getComment(),"Comment is empty");
	    		assertEquals("",actual[5].getFormula(),"Formula contains data type");
	    		assertEquals("F1",actual[5].getAddress(),"Address is correct");
	    		assertEquals("3.5",actual[6].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[6].getComment(),"Comment is empty");
	    		assertEquals("",actual[6].getFormula(),"Formula contains data type");
	    		assertEquals("G1",actual[6].getAddress(),"Address is correct");
	    		assertEquals("1.5",actual[7].getFormattedValue(),"Formatted Value is empty");
	    		assertEquals("",actual[7].getComment(),"Comment is empty");
	    		assertEquals("",actual[7].getFormula(),"Formula contains data type");
	    		assertEquals("H1",actual[7].getAddress(),"Address is correct");
	    		assertEquals("1/13/18",actual[8].getFormattedValue(),"Formatted Value contains data type");
	    		assertEquals("",actual[8].getComment(),"Comment is empty");
	    		assertEquals("",actual[8].getFormula(),"Formula is empty");
	    		assertEquals("I1",actual[8].getAddress(),"Address is correct");
	    		assertEquals("1/13/18",actual[9].getFormattedValue(),"Formatted Value contains data type");
	    		assertEquals("",actual[9].getComment(),"Comment is empty");
	    		assertEquals("",actual[9].getFormula(),"Formula is empty");
	    		assertEquals("J1",actual[9].getAddress(),"Address is correct");
	    		assertEquals("2018-01-13 00:00:00.0",actual[10].getFormattedValue(),"Formatted Value contains data type");
	    		assertEquals("",actual[10].getComment(),"Comment is empty");
	    		assertEquals("",actual[10].getFormula(),"Formula is empty");
	    		assertEquals("K1",actual[10].getAddress(),"Address is correct");
	    		assertEquals("This is a test",actual[11].getFormattedValue(),"Formatted Value contains data");
	    		assertEquals("",actual[11].getComment(),"Comment is empty");
	    		assertEquals("",actual[11].getFormula(),"Formula is empty");
	    		assertEquals("L1",actual[11].getAddress(),"Address is correct");
	    		assertNull(actual[12],"Null values stay null");
	    }
	    
	    
	    /**
	     * https://github.com/ZuInnoTe/hadoopoffice/issues/42
	     * 
	     * Numbers in scientific notations are converted correctly
	     * 
	     */
	    @Test
	    public void testScientificNotation() {
	
	    	// configure converter
    		SimpleDateFormat dateFormat = (SimpleDateFormat)DateFormat.getDateInstance(DateFormat.SHORT, Locale.US);
    		SimpleDateFormat timeStampFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    		
    		DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(Locale.GERMAN);
    		ExcelConverterSimpleSpreadSheetCellDAO converter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat,decimalFormat,timeStampFormat);
    		GenericDataType[] schema = new GenericDataType[2];
    		// create schema
    		schema[0] = new GenericBigDecimalDataType(38,19);
    		schema[1] = new GenericBigDecimalDataType(38,19);
    		converter.setSchemaRow(schema);
    		// create SpreadSheetCellDAOs
    		SpreadSheetCellDAO[] data = new SpreadSheetCellDAO[2];
    		data[0] = new SpreadSheetCellDAO("4.03578E+14","","","A1","Sheet1");
    		data[1] = new SpreadSheetCellDAO("1,23457E+20","","","B1","Sheet1");
    		Object[] primitiveData = converter.getDataAccordingToSchema(data);
    		assertEquals("403578000000000",((BigDecimal)primitiveData[0]).toPlainString(),"Scientific notation (UK) is correctly converted");
    		assertEquals("123457000000000000000",((BigDecimal)primitiveData[1]).toPlainString(),"Scientific notation (DE) is correctly converted");
	    }

}

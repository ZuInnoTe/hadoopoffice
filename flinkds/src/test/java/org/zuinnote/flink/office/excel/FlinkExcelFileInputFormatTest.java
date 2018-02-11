/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.flink.office.excel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

/**
 * @author jornfranke
 *
 */
public class FlinkExcelFileInputFormatTest {

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
	    public void checkTestExcel2003SingleSheetAvailable() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="excel2003test.xls";
			String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
			assertNotNull(fileNameSpreadSheet,"Test Data File \""+fileName+"\" is not null in resource path");
			File file = new File(fileNameSpreadSheet);
			assertTrue(file.exists(),"Test Data File \""+fileName+"\" exists");
			assertFalse(file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	    }

	    @Test
	    public void checkTestExcel2013SingleSheetAvailable() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="excel2013test.xlsx";
			String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
			assertNotNull(fileNameSpreadSheet,"Test Data File \""+fileName+"\" is not null in resource path");
			File file = new File(fileNameSpreadSheet);
			assertTrue(file.exists(),"Test Data File \""+fileName+"\" exists");
			assertFalse(file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
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
	    public void readExcel2003SingleSheet() throws IOException {

			  ClassLoader classLoader = getClass().getClassLoader();
			    String fileName="excel2003test.xls";
			    String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
			    Path file = new Path(fileNameSpreadSheet); 
			    FileInputSplit spreadSheetInputSplit = new FileInputSplit(0,file,0, -1, null);
			    HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
			    hocr.setLocale(Locale.US);
			    boolean useHeader=false;
			    ExcelFlinkFileInputFormat inputFormat = new ExcelFlinkFileInputFormat(hocr,useHeader);
			    inputFormat.open(spreadSheetInputSplit);
			    assertFalse(inputFormat.reachedEnd(),"End not reached");
			    SpreadSheetCellDAO[] reuse = new  SpreadSheetCellDAO[0];
			    SpreadSheetCellDAO[] nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"First Row returned");
			    assertEquals(4, nextRow.length, "First row has 4 columns");
			    assertEquals("test1", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
				assertEquals("Sheet1",  nextRow[0].getSheetName(),
						"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
				assertEquals("A1",  nextRow[0].getAddress(),
						"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
				assertEquals("test2",  nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
				assertEquals("test3",  nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
				assertEquals("test4",  nextRow[3].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Second Row returned");
			    assertEquals(1, nextRow.length, "Input Split for Excel file contains row 2 with 1 column");
				assertEquals("4", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Third Row returned");
			    assertEquals(5, nextRow.length, "Input Split for Excel file contains row 3 with 5 columns");
				assertEquals("31/12/99", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
				assertEquals("5",  nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
				assertNull(nextRow[2], "Input Split for Excel file contains row 3 with cell 3 == null");
				assertNull(nextRow[3], "Input Split for Excel file contains row 3 with cell 4 == null");
				assertEquals("null",  nextRow[4].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Fourth Row returned");
			    assertEquals(1, nextRow.length, "Input Split for Excel file contains row 4 with 1 column");
				assertEquals("1", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Fifth Row returned");
			    assertEquals(3, nextRow.length, "Input Split for Excel file contains row 5 with 3 columns");
				assertEquals("2", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
				assertEquals("6", nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 2== \"6\"");
				assertEquals("10", nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 3== \"10\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Sixth Row returned");
				assertEquals(3, nextRow.length, "Input Split for Excel file contains row 6 with 3 columns");
				assertEquals("3", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
				assertEquals("4", nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 2== \"4\"");
				assertEquals("15", nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 3== \"15\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertTrue(inputFormat.reachedEnd(),"End reached");
	    }
	    
	    @Test
	    public void readExcel2013SingleSheet() throws IOException {

			   ClassLoader classLoader = getClass().getClassLoader();
			    String fileName="excel2013test.xlsx";
			    String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
			    Path file = new Path(fileNameSpreadSheet); 
			    FileInputSplit spreadSheetInputSplit = new FileInputSplit(0,file,0, -1, null);
			    HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
			    hocr.setLocale(Locale.US);
			    boolean useHeader=false;
			    ExcelFlinkFileInputFormat inputFormat = new ExcelFlinkFileInputFormat(hocr,useHeader);
			    inputFormat.open(spreadSheetInputSplit);
			    assertFalse(inputFormat.reachedEnd(),"End not reached");
			    SpreadSheetCellDAO[] reuse = new  SpreadSheetCellDAO[0];
			    SpreadSheetCellDAO[] nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"First Row returned");
			    assertEquals(4, nextRow.length, "First row has 4 columns");
			    assertEquals("test1", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
				assertEquals("Sheet1",  nextRow[0].getSheetName(),
						"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
				assertEquals("A1",  nextRow[0].getAddress(),
						"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
				assertEquals("test2",  nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
				assertEquals("test3",  nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
				assertEquals("test4",  nextRow[3].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Second Row returned");
			    assertEquals(1, nextRow.length, "Input Split for Excel file contains row 2 with 1 column");
				assertEquals("4", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Third Row returned");
			    assertEquals(5, nextRow.length, "Input Split for Excel file contains row 3 with 5 columns");
				assertEquals("31/12/99", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
				assertEquals("5",  nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
				assertNull(nextRow[2], "Input Split for Excel file contains row 3 with cell 3 == null");
				assertNull(nextRow[3], "Input Split for Excel file contains row 3 with cell 4 == null");
				assertEquals("null",  nextRow[4].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Fourth Row returned");
			    assertEquals(1, nextRow.length, "Input Split for Excel file contains row 4 with 1 column");
				assertEquals("1", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Fifth Row returned");
			    assertEquals(3, nextRow.length, "Input Split for Excel file contains row 5 with 3 columns");
				assertEquals("2", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
				assertEquals("6", nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 2== \"6\"");
				assertEquals("10", nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 3== \"10\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Sixth Row returned");
				assertEquals(3, nextRow.length, "Input Split for Excel file contains row 6 with 3 columns");
				assertEquals("3", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
				assertEquals("4", nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 2== \"4\"");
				assertEquals("15", nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 3== \"15\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertTrue(inputFormat.reachedEnd(),"End reached");
	    }
	    

	    @Test
	    public void restoreExcel2013SingleSheet() throws IOException {
	    	 ClassLoader classLoader = getClass().getClassLoader();
			    String fileName="excel2013test.xlsx";
			    String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
			    Path file = new Path(fileNameSpreadSheet); 
			    FileInputSplit spreadSheetInputSplit = new FileInputSplit(0,file,0, -1, null);
			    HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
			    hocr.setLocale(Locale.US);
			    boolean useHeader=false;
			    ExcelFlinkFileInputFormat inputFormat = new ExcelFlinkFileInputFormat(hocr,useHeader);
			    inputFormat.open(spreadSheetInputSplit);
			    assertFalse(inputFormat.reachedEnd(),"End not reached");
			    SpreadSheetCellDAO[] reuse = new  SpreadSheetCellDAO[0];
			    SpreadSheetCellDAO[] nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"First Row returned");
			    assertEquals(4, nextRow.length, "First row has 4 columns");
			    assertEquals("test1", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
				assertEquals("Sheet1",  nextRow[0].getSheetName(),
						"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
				assertEquals("A1",  nextRow[0].getAddress(),
						"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
				assertEquals("test2",  nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
				assertEquals("test3",  nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
				assertEquals("test4",  nextRow[3].getFormattedValue(),
						"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Second Row returned");
			    assertEquals(1, nextRow.length, "Input Split for Excel file contains row 2 with 1 column");
				assertEquals("4", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
				// store state
				Tuple2<Long,Long> state = inputFormat.getCurrentState();
				assertEquals(0,(long)state.f0,"sheet num: 0");
				assertEquals(2,(long)state.f1,"row num: 2");
				// continue reading
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Third Row returned");
			    assertEquals(5, nextRow.length, "Input Split for Excel file contains row 3 with 5 columns");
				assertEquals("31/12/99", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
				assertEquals("5",  nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
				assertNull(nextRow[2], "Input Split for Excel file contains row 3 with cell 3 == null");
				assertNull(nextRow[3], "Input Split for Excel file contains row 3 with cell 4 == null");
				assertEquals("null",  nextRow[4].getFormattedValue(),
						"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
				// restore state
				 inputFormat.reopen(spreadSheetInputSplit, state);
				 nextRow = inputFormat.nextRecord(reuse);
				 assertNotNull(nextRow,"(Restore) Third Row returned");
				assertEquals(5, nextRow.length, "(Restore) Input Split for Excel file contains row 3 with 5 columns");
				assertEquals("31/12/99", nextRow[0].getFormattedValue(),
							"(Restore) Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
				assertEquals("5",  nextRow[1].getFormattedValue(),
							"(Restore) Input Split for Excel file contains row 3 with cell 2 == \"5\"");
				assertNull(nextRow[2], "(Restore) Input Split for Excel file contains row 3 with cell 3 == null");
				assertNull(nextRow[3], "(Restore) Input Split for Excel file contains row 3 with cell 4 == null");
				assertEquals("null",  nextRow[4].getFormattedValue(),
							"(Restore) Input Split for Excel file contains row 3 with cell 5 == \"null\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Fourth Row returned");
			    assertEquals(1, nextRow.length, "Input Split for Excel file contains row 4 with 1 column");
				assertEquals("1", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Fifth Row returned");
			    assertEquals(3, nextRow.length, "Input Split for Excel file contains row 5 with 3 columns");
				assertEquals("2", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
				assertEquals("6", nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 2== \"6\"");
				assertEquals("10", nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 5 with cell 3== \"10\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertNotNull(nextRow,"Sixth Row returned");
				assertEquals(3, nextRow.length, "Input Split for Excel file contains row 6 with 3 columns");
				assertEquals("3", nextRow[0].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
				assertEquals("4", nextRow[1].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 2== \"4\"");
				assertEquals("15", nextRow[2].getFormattedValue(),
						"Input Split for Excel file contains row 6 with cell 3== \"15\"");
			    nextRow = inputFormat.nextRecord(reuse);
			    assertTrue(inputFormat.reachedEnd(),"End reached");
	    }
}

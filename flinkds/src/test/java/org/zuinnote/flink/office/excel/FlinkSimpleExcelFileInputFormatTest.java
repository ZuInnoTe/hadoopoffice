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
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
public class FlinkSimpleExcelFileInputFormatTest {
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
		String fileName = "excel2003test.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013SingleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcelSimpleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "testsimple.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void readSimpleExcel2013() throws IOException, ParseException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "testsimple.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputSplit spreadSheetInputSplit = new FileInputSplit(0, file, 0, -1, null);
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		SimpleDateFormat dateFormat = (SimpleDateFormat) DateFormat.getDateInstance(DateFormat.SHORT, Locale.US);
		DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(Locale.GERMAN);
		boolean useHeader = true;
		SimpleExcelFlinkFileInputFormat inputFormat = new SimpleExcelFlinkFileInputFormat(hocr, -1, useHeader,
				dateFormat, decimalFormat);
		inputFormat.open(spreadSheetInputSplit);
		assertFalse(inputFormat.reachedEnd(), "End not reached");
		Object[] reuse = new Object[0];
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		// check header
		String[] header = inputFormat.getHeader();
		assertEquals("decimalsc1", header[0], "Correct Header Column");
		assertEquals("booleancolumn", header[1], "Correct Header Column");
		assertEquals("datecolumn", header[2], "Correct Header Column");
		assertEquals("stringcolumn", header[3], "Correct Header Column");
		assertEquals("decimalp8sc3", header[4], "Correct Header Column");
		assertEquals("bytecolumn", header[5], "Correct Header Column");
		assertEquals("shortcolumn", header[6], "Correct Header Column");
		assertEquals("intcolumn", header[7], "Correct Header Column");
		assertEquals("longcolumn", header[8], "Correct Header Column");

		// check data
		Object[] simpleRow1 = inputFormat.nextRecord(reuse);

		assertEquals(new BigDecimal("1.00"), simpleRow1[0], "A2 = 1.00");
		assertTrue((Boolean) simpleRow1[1], "B2 = TRUE");
		assertEquals(sdf.parse("2017-01-01"), simpleRow1[2], "C2 = 2017-01-01");
		assertEquals("This is a text", simpleRow1[3], "D2 = This is a text");
		assertEquals(new BigDecimal("10.000"), simpleRow1[4], "E2 = 10.000");
		assertEquals((byte) 3, simpleRow1[5], "F2 = 3");
		assertEquals((short) 3, simpleRow1[6], "G2 = 3");
		assertEquals((int) 100, simpleRow1[7], "H2 = 100");
		assertEquals(65335L, simpleRow1[8], "I2 = 65335");
		Object[] simpleRow2 = inputFormat.nextRecord(reuse);

		assertEquals(new BigDecimal("1.50"), simpleRow2[0], "A3 = 1.50");
		assertFalse((Boolean) simpleRow2[1], "B3 = FALSE");
		assertEquals(sdf.parse("2017-02-28"), simpleRow2[2], "C3 = 2017-02-28");
		assertEquals("Another String", simpleRow2[3], "D3 = Another String");
		assertEquals(new BigDecimal("2.334"), simpleRow2[4], "E3 = 2.334");
		assertEquals((byte) 5, simpleRow2[5], "F3 = 5");
		assertEquals((short) 4, simpleRow2[6], "G3 = 4");
		assertEquals((int) 65335, simpleRow2[7], "H3 = 65335");
		assertEquals(1L, simpleRow2[8], "I3 = 1");
		Object[] simpleRow3 = inputFormat.nextRecord(reuse);

		assertEquals(new BigDecimal("3.40"), simpleRow3[0], "A4 = 3.40");
		assertFalse((Boolean) simpleRow3[1], "B4 = FALSE");
		assertEquals(sdf.parse("2000-02-29"), simpleRow3[2], "C4 = 2000-02-29");
		assertEquals("10", simpleRow3[3], "D4 = 10");
		assertEquals(new BigDecimal("4.500"), simpleRow3[4], "E4 = 4.500");
		assertEquals((byte) -100, simpleRow3[5], "F4 = -100");
		assertEquals((short) 5, simpleRow3[6], "G4 = 5");
		assertEquals((int) 1, simpleRow3[7], "H4 = 1");
		assertEquals(250L, simpleRow3[8], "I4 = 250");
		Object[] simpleRow4 = inputFormat.nextRecord(reuse);
		assertEquals(new BigDecimal("5.50"), simpleRow4[0], "A5 = 5.50");
		assertFalse((Boolean) simpleRow4[1], "B5 = FALSE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow4[2], "C5 = 2017-03-01");
		assertEquals("test3", simpleRow4[3], "D5 = test3");
		assertEquals(new BigDecimal("11.000"), simpleRow4[4], "E5 = 11.000");
		assertEquals((byte) 2, simpleRow4[5], "F5 = 2");
		assertEquals((short) 250, simpleRow4[6], "G5 = 250");
		assertEquals((int) 250, simpleRow4[7], "H5 = 250");
		assertEquals(10L, simpleRow4[8], "I5 = 10");
		Object[] simpleRow5 = inputFormat.nextRecord(reuse);
		assertNull(simpleRow5[0], "A6 = null");
		assertNull(simpleRow5[1], "B6 = null");
		assertNull(simpleRow5[2], "C6 = null");
		assertEquals("test4", simpleRow5[3], "D6 = test4");
		assertEquals(new BigDecimal("100.000"), simpleRow5[4], "E6 = 100.000");
		assertEquals((byte) 3, simpleRow5[5], "F6 = 3");
		assertEquals((short) 3, simpleRow5[6], "G6 = 3");
		assertEquals((int) 5, simpleRow5[7], "H6 = 5");
		assertEquals(3147483647L, simpleRow5[8], "I6 = 3147483647");
		Object[] simpleRow6 = inputFormat.nextRecord(reuse);
		assertEquals(new BigDecimal("3.40"), simpleRow6[0], "A7 = 3.40");
		assertTrue((Boolean) simpleRow6[1], "B7 = TRUE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow6[2], "C7 = 2017-03-01");
		assertEquals("test5", simpleRow6[3], "D7 = test5");
		assertEquals(new BigDecimal("10000.500"), simpleRow6[4], "E6 = 10000.500");
		assertEquals((byte) 120, simpleRow6[5], "F7 = 120");
		assertEquals((short) 100, simpleRow6[6], "G7 = 100");
		assertEquals((int) 10000, simpleRow6[7], "H7 = 10000");
		assertEquals(10L, simpleRow6[8], "I6 = 10");
		inputFormat.nextRecord(reuse);
		assertTrue(inputFormat.reachedEnd(), "End reached");
	}

	public void restoreSimpleExcel2013() throws ParseException, IOException {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "testsimple.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputSplit spreadSheetInputSplit = new FileInputSplit(0, file, 0, -1, null);
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		SimpleDateFormat dateFormat = (SimpleDateFormat) DateFormat.getDateInstance(DateFormat.SHORT, Locale.US);
		DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(Locale.GERMAN);
		boolean useHeader = true;
		SimpleExcelFlinkFileInputFormat inputFormat = new SimpleExcelFlinkFileInputFormat(hocr, -1, useHeader,
				dateFormat, decimalFormat);
		inputFormat.open(spreadSheetInputSplit);
		assertFalse(inputFormat.reachedEnd(), "End not reached");
		Object[] reuse = new Object[0];
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		// check header
		String[] header = inputFormat.getHeader();
		assertEquals("decimalsc1", header[0], "Correct Header Column");
		assertEquals("booleancolumn", header[1], "Correct Header Column");
		assertEquals("datecolumn", header[2], "Correct Header Column");
		assertEquals("stringcolumn", header[3], "Correct Header Column");
		assertEquals("decimalp8sc3", header[4], "Correct Header Column");
		assertEquals("bytecolumn", header[5], "Correct Header Column");
		assertEquals("shortcolumn", header[6], "Correct Header Column");
		assertEquals("intcolumn", header[7], "Correct Header Column");
		assertEquals("longcolumn", header[8], "Correct Header Column");

		// check data
		Object[] simpleRow1 = inputFormat.nextRecord(reuse);

		assertEquals(new BigDecimal("1.00"), simpleRow1[0], "A2 = 1.00");
		assertTrue((Boolean) simpleRow1[1], "B2 = TRUE");
		assertEquals(sdf.parse("2017-01-01"), simpleRow1[2], "C2 = 2017-01-01");
		assertEquals("This is a text", simpleRow1[3], "D2 = This is a text");
		assertEquals(new BigDecimal("10.000"), simpleRow1[4], "E2 = 10.000");
		assertEquals((byte) 3, simpleRow1[5], "F2 = 3");
		assertEquals((short) 3, simpleRow1[6], "G2 = 3");
		assertEquals((int) 100, simpleRow1[7], "H2 = 100");
		assertEquals(65335L, simpleRow1[8], "I2 = 65335");
		Object[] simpleRow2 = inputFormat.nextRecord(reuse);

		assertEquals(new BigDecimal("1.50"), simpleRow2[0], "A3 = 1.50");
		assertFalse((Boolean) simpleRow2[1], "B3 = FALSE");
		assertEquals(sdf.parse("2017-02-28"), simpleRow2[2], "C3 = 2017-02-28");
		assertEquals("Another String", simpleRow2[3], "D3 = Another String");
		assertEquals(new BigDecimal("2.334"), simpleRow2[4], "E3 = 2.334");
		assertEquals((byte) 5, simpleRow2[5], "F3 = 5");
		assertEquals((short) 4, simpleRow2[6], "G3 = 4");
		assertEquals((int) 65335, simpleRow2[7], "H3 = 65335");
		assertEquals(1L, simpleRow2[8], "I3 = 1");
		// store state
		Tuple2<Long, Long> state = inputFormat.getCurrentState();
		assertEquals(0, (long) state.f0, "sheet num: 0");
		assertEquals(2, (long) state.f1, "row num: 2");
		Object[] simpleRow3 = inputFormat.nextRecord(reuse);
		assertEquals(2, (long) state.f1, "row num: 2");
		assertEquals(new BigDecimal("3.40"), simpleRow3[0], "A4 = 3.40");
		assertFalse((Boolean) simpleRow3[1], "B4 = FALSE");
		assertEquals(sdf.parse("2000-02-29"), simpleRow3[2], "C4 = 2000-02-29");
		assertEquals("10", simpleRow3[3], "D4 = 10");
		assertEquals(new BigDecimal("4.500"), simpleRow3[4], "E4 = 4.500");
		assertEquals((byte) -100, simpleRow3[5], "F4 = -100");
		assertEquals((short) 5, simpleRow3[6], "G4 = 5");
		assertEquals((int) 1, simpleRow3[7], "H4 = 1");
		assertEquals(250L, simpleRow3[8], "I4 = 250");
		// restore state
		inputFormat.reopen(spreadSheetInputSplit, state);
		Object[] simpleRow3recover = inputFormat.nextRecord(reuse);
		assertEquals(new BigDecimal("3.40"), simpleRow3recover[0], "A4 = 3.40");
		assertFalse((Boolean) simpleRow3recover[1], "B4 = FALSE");
		assertEquals(sdf.parse("2000-02-29"), simpleRow3recover[2], "C4 = 2000-02-29");
		assertEquals("10", simpleRow3recover[3], "D4 = 10");
		assertEquals(new BigDecimal("4.500"), simpleRow3recover[4], "E4 = 4.500");
		assertEquals((byte) -100, simpleRow3recover[5], "F4 = -100");
		assertEquals((short) 5, simpleRow3recover[6], "G4 = 5");
		assertEquals((int) 1, simpleRow3recover[7], "H4 = 1");
		assertEquals(250L, simpleRow3recover[8], "I4 = 250");
		// continue business as usual
		Object[] simpleRow4 = inputFormat.nextRecord(reuse);
		assertEquals(new BigDecimal("5.50"), simpleRow4[0], "A5 = 5.50");
		assertFalse((Boolean) simpleRow4[1], "B5 = FALSE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow4[2], "C5 = 2017-03-01");
		assertEquals("test3", simpleRow4[3], "D5 = test3");
		assertEquals(new BigDecimal("11.000"), simpleRow4[4], "E5 = 11.000");
		assertEquals((byte) 2, simpleRow4[5], "F5 = 2");
		assertEquals((short) 250, simpleRow4[6], "G5 = 250");
		assertEquals((int) 250, simpleRow4[7], "H5 = 250");
		assertEquals(10L, simpleRow4[8], "I5 = 10");
		Object[] simpleRow5 = inputFormat.nextRecord(reuse);
		assertNull(simpleRow5[0], "A6 = null");
		assertNull(simpleRow5[1], "B6 = null");
		assertNull(simpleRow5[2], "C6 = null");
		assertEquals("test4", simpleRow5[3], "D6 = test4");
		assertEquals(new BigDecimal("100.000"), simpleRow5[4], "E6 = 100.000");
		assertEquals((byte) 3, simpleRow5[5], "F6 = 3");
		assertEquals((short) 3, simpleRow5[6], "G6 = 3");
		assertEquals((int) 5, simpleRow5[7], "H6 = 5");
		assertEquals(3147483647L, simpleRow5[8], "I6 = 3147483647");
		Object[] simpleRow6 = inputFormat.nextRecord(reuse);
		assertEquals(new BigDecimal("3.40"), simpleRow6[0], "A7 = 3.40");
		assertTrue((Boolean) simpleRow6[1], "B7 = TRUE");
		assertEquals(sdf.parse("2017-03-01"), simpleRow6[2], "C7 = 2017-03-01");
		assertEquals("test5", simpleRow6[3], "D7 = test5");
		assertEquals(new BigDecimal("10000.500"), simpleRow6[4], "E6 = 10000.500");
		assertEquals((byte) 120, simpleRow6[5], "F7 = 120");
		assertEquals((short) 100, simpleRow6[6], "G7 = 100");
		assertEquals((int) 10000, simpleRow6[7], "H7 = 10000");
		assertEquals(10L, simpleRow6[8], "I6 = 10");
		inputFormat.nextRecord(reuse);
		assertTrue(inputFormat.reachedEnd(), "End reached");
	}
}

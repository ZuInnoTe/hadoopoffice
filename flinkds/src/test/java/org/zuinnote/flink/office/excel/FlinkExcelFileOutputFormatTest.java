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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

/**
 * @author jornfranke
 *
 */
public class FlinkExcelFileOutputFormatTest {

	private static final String tmpPrefix = "flinkofficetest";
	private static java.nio.file.Path tmpPath;
	public static final String MIMETYPE_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
	public static final String MIMETYPE_XLS = "application/vnd.ms-excel";

	@BeforeAll
	public static void oneTimeSetUp() throws IOException {
		// one-time initialization code
		// create temp directory
		tmpPath = Files.createTempDirectory(tmpPrefix);

		// create shutdown hook to remove temp files after shutdown, may need to rethink
		// to avoid many threads are created
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Files.walkFileTree(tmpPath, new SimpleFileVisitor<java.nio.file.Path>() {

						@Override
						public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
								throws IOException {
							Files.delete(file);
							return FileVisitResult.CONTINUE;
						}

						@Override
						public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException e)
								throws IOException {
							if (e == null) {
								Files.delete(dir);
								return FileVisitResult.CONTINUE;
							}
							throw e;
						}
					});
				} catch (IOException e) {
					throw new RuntimeException(
							"Error temporary files in following path could not be deleted " + tmpPath, e);
				}
			}
		}));
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
	public void writeExcel2003SingleSheetWithoutHeader() throws IOException {
		// write
		String fileName = "excel2003singlesheetout.xls";
		Path file = new Path(this.tmpPath.toString(), fileName);
		HadoopOfficeWriteConfiguration howc = new HadoopOfficeWriteConfiguration("1");
		howc.setMimeType(FlinkExcelFileOutputFormatTest.MIMETYPE_XLS);
		String[] header = null;
		String defaultSheetName = "Sheet1";
		ExcelFlinkFileOutputFormat outputFormat = new ExcelFlinkFileOutputFormat(howc, header, defaultSheetName);
		outputFormat.setOutputFilePath(file);
		outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		outputFormat.initializeGlobal(1);

		outputFormat.open(0, 2);
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// empty row => nothing todo
		// one row numbers (1,2,3)
		SpreadSheetCellDAO a3 = new SpreadSheetCellDAO("", "", "1", "A3", "Sheet1");
		SpreadSheetCellDAO b3 = new SpreadSheetCellDAO("", "", "2", "B3", "Sheet1");
		SpreadSheetCellDAO c3 = new SpreadSheetCellDAO("", "", "3", "C3", "Sheet1");
		// one row formulas (=A3+B3)
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("", "", "A3+B3", "A4", "Sheet1");
		SpreadSheetCellDAO[] row1 = new SpreadSheetCellDAO[] { a1, b1, c1 };
		SpreadSheetCellDAO[] row2 = new SpreadSheetCellDAO[] { a3, b3, c3 };
		SpreadSheetCellDAO[] row3 = new SpreadSheetCellDAO[] { a4 };
		outputFormat.writeRecord(row1);
		outputFormat.writeRecord(row2);
		outputFormat.writeRecord(row3);
		outputFormat.close();
		// Read the file back
		FileInputSplit spreadSheetInputSplit = new FileInputSplit(0, new Path(file, "1"), 0, -1, null);
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setLocale(Locale.US);
		boolean useHeader = false;
		ExcelFlinkFileInputFormat inputFormat = new ExcelFlinkFileInputFormat(hocr, useHeader);
		inputFormat.open(spreadSheetInputSplit);
		assertFalse(inputFormat.reachedEnd(), "End not reached");
		SpreadSheetCellDAO[] reuse = new SpreadSheetCellDAO[0];
		SpreadSheetCellDAO[] nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "First Row returned");
		assertEquals(3, nextRow.length, "First row has 3 columns");
		assertEquals("test1", nextRow[0].getFormattedValue(), "1th Row 1th Cell formattedValue = \"test1\"");
		assertEquals("A1", nextRow[0].getAddress(), "1th Row 1th Cell address = \"A1\"");
		assertEquals("", nextRow[0].getComment(), "1th Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "1th Row 1th cell Sheet = \"Sheet1\"");
		assertEquals("test2", nextRow[1].getFormattedValue(), "1th Row 2nd Cell formattedValue = \"test2\"");
		assertEquals("B1", nextRow[1].getAddress(), "1th Row 2nd Cell address = \"B1\"");
		assertEquals("", nextRow[1].getComment(), "1th Row 2nd Comment = \"\"");
		assertEquals("Sheet1", nextRow[1].getSheetName(), "1th Row 2nd Cell Sheet = \"Sheet1\"");
		assertEquals("test3", nextRow[2].getFormattedValue(), "1th Row 3rd Cell formattedValue = \"test3\"");
		assertEquals("C1", nextRow[2].getAddress(), "1th Row 3rd Celll address = \"C1\"");
		assertEquals("", nextRow[2].getComment(), "1th Row 3rd Cell Comment = \"\"");
		assertEquals("Sheet1", nextRow[2].getSheetName(), "1th Row 3rd Cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Second Row returned");
		assertEquals(0, nextRow.length, "Second row is empty");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Third Row returned");
		assertEquals(3, nextRow.length, "Third row has 3 columns");
		assertEquals("1", nextRow[0].getFormattedValue(), "3rd Row 1th Cell formattedValue = \"1\"");
		assertEquals("A3", nextRow[0].getAddress(), "3rd Row 1th Cell address = \"A3\"");
		assertEquals("", nextRow[0].getComment(), "3rd Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "3rd Row 1th cell Sheet = \"Sheet1\"");
		assertEquals("2", nextRow[1].getFormattedValue(), "3rd Row 2nd Cell formattedValue = \"2\"");
		assertEquals("B3", nextRow[1].getAddress(), "3rd Row 2nd Cell address = \"B3\"");
		assertEquals("", nextRow[1].getComment(), "3rd Row 2nd Comment = \"\"");
		assertEquals("Sheet1", nextRow[1].getSheetName(), "3rd Row 2nd Cell Sheet = \"Sheet1\"");
		assertEquals("3", nextRow[2].getFormattedValue(), "3rd Row 3rd Cell formattedValue = \"3\"");
		assertEquals("C3", nextRow[2].getAddress(), "3rd Row 3rd Celll address = \"C3\"");
		assertEquals("", nextRow[2].getComment(), "3rd Row 3rd Cell Comment = \"\"");
		assertEquals("Sheet1", nextRow[2].getSheetName(), "3rd Row 3rd Cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Forth Row returned");
		assertEquals(1, nextRow.length, "Forth row has 1 columns");
		assertEquals("3", nextRow[0].getFormattedValue(), "4th Row 1th Cell formattedValue = \"3\"");
		assertEquals("A4", nextRow[0].getAddress(), "4th Row 1th Cell address = \"A4\"");
		assertEquals("", nextRow[0].getComment(), "4th Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "4th Row 1th cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertTrue(inputFormat.reachedEnd(), "End reached");
		inputFormat.close();
	}
	
	@Test
	public void writeExcel2003SingleSheetWithHeader() throws IOException {
		// write
		String fileName = "excel2003singlesheetoutwithhead.xls";
		Path file = new Path(this.tmpPath.toString(), fileName);
		HadoopOfficeWriteConfiguration howc = new HadoopOfficeWriteConfiguration("1");
		howc.setMimeType(FlinkExcelFileOutputFormatTest.MIMETYPE_XLS);
		String[] header = new String[] {"test1","test2","test3"};
		String defaultSheetName = "Sheet1";
		ExcelFlinkFileOutputFormat outputFormat = new ExcelFlinkFileOutputFormat(howc, header, defaultSheetName);
		outputFormat.setOutputFilePath(file);
		outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		outputFormat.initializeGlobal(1);

		outputFormat.open(0, 2);
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		// empty row => nothing todo
		// one row numbers (1,2,3)
		SpreadSheetCellDAO a3 = new SpreadSheetCellDAO("", "", "1", "A3", "Sheet1");
		SpreadSheetCellDAO b3 = new SpreadSheetCellDAO("", "", "2", "B3", "Sheet1");
		SpreadSheetCellDAO c3 = new SpreadSheetCellDAO("", "", "3", "C3", "Sheet1");
		// one row formulas (=A3+B3)
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("", "", "A3+B3", "A4", "Sheet1");
		SpreadSheetCellDAO[] row2 = new SpreadSheetCellDAO[] { a3, b3, c3 };
		SpreadSheetCellDAO[] row3 = new SpreadSheetCellDAO[] { a4 };
		outputFormat.writeRecord(row2);
		outputFormat.writeRecord(row3);
		outputFormat.close();
		// Read the file back
		FileInputSplit spreadSheetInputSplit = new FileInputSplit(0, new Path(file, "1"), 0, -1, null);
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setLocale(Locale.US);
		boolean useHeader = true;
		ExcelFlinkFileInputFormat inputFormat = new ExcelFlinkFileInputFormat(hocr, useHeader);
		inputFormat.open(spreadSheetInputSplit);
		assertFalse(inputFormat.reachedEnd(), "End not reached");
		String[] expectedHeader = new String[] {"test1","test2","test3"};
		assertArrayEquals(expectedHeader,inputFormat.getHeader(),"Header is correctly read");
		SpreadSheetCellDAO[] reuse = new SpreadSheetCellDAO[0];
		SpreadSheetCellDAO[] nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Second Row returned");
		assertEquals(0, nextRow.length, "Second row is empty");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Third Row returned");
		assertEquals(3, nextRow.length, "Third row has 3 columns");
		assertEquals("1", nextRow[0].getFormattedValue(), "3rd Row 1th Cell formattedValue = \"1\"");
		assertEquals("A3", nextRow[0].getAddress(), "3rd Row 1th Cell address = \"A3\"");
		assertEquals("", nextRow[0].getComment(), "3rd Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "3rd Row 1th cell Sheet = \"Sheet1\"");
		assertEquals("2", nextRow[1].getFormattedValue(), "3rd Row 2nd Cell formattedValue = \"2\"");
		assertEquals("B3", nextRow[1].getAddress(), "3rd Row 2nd Cell address = \"B3\"");
		assertEquals("", nextRow[1].getComment(), "3rd Row 2nd Comment = \"\"");
		assertEquals("Sheet1", nextRow[1].getSheetName(), "3rd Row 2nd Cell Sheet = \"Sheet1\"");
		assertEquals("3", nextRow[2].getFormattedValue(), "3rd Row 3rd Cell formattedValue = \"3\"");
		assertEquals("C3", nextRow[2].getAddress(), "3rd Row 3rd Celll address = \"C3\"");
		assertEquals("", nextRow[2].getComment(), "3rd Row 3rd Cell Comment = \"\"");
		assertEquals("Sheet1", nextRow[2].getSheetName(), "3rd Row 3rd Cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Forth Row returned");
		assertEquals(1, nextRow.length, "Forth row has 1 columns");
		assertEquals("3", nextRow[0].getFormattedValue(), "4th Row 1th Cell formattedValue = \"3\"");
		assertEquals("A4", nextRow[0].getAddress(), "4th Row 1th Cell address = \"A4\"");
		assertEquals("", nextRow[0].getComment(), "4th Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "4th Row 1th cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertTrue(inputFormat.reachedEnd(), "End reached");
		inputFormat.close();
	}

	@Test
	public void writeExcel2013SingleSheetWithoutheader() throws IOException {
		// write
		String fileName = "excel2013singlesheetout.xlsx";
		Path file = new Path(this.tmpPath.toString(), fileName);
		HadoopOfficeWriteConfiguration howc = new HadoopOfficeWriteConfiguration("1");
		howc.setMimeType(FlinkExcelFileOutputFormatTest.MIMETYPE_XLSX);
		String[] header = null;
		String defaultSheetName = "Sheet1";
		ExcelFlinkFileOutputFormat outputFormat = new ExcelFlinkFileOutputFormat(howc, header, defaultSheetName);
		outputFormat.setOutputFilePath(file);
		outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		outputFormat.initializeGlobal(1);

		outputFormat.open(0, 2);
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// empty row => nothing todo
		// one row numbers (1,2,3)
		SpreadSheetCellDAO a3 = new SpreadSheetCellDAO("", "", "1", "A3", "Sheet1");
		SpreadSheetCellDAO b3 = new SpreadSheetCellDAO("", "", "2", "B3", "Sheet1");
		SpreadSheetCellDAO c3 = new SpreadSheetCellDAO("", "", "3", "C3", "Sheet1");
		// one row formulas (=A3+B3)
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("", "", "A3+B3", "A4", "Sheet1");
		SpreadSheetCellDAO[] row1 = new SpreadSheetCellDAO[] { a1, b1, c1 };
		SpreadSheetCellDAO[] row2 = new SpreadSheetCellDAO[] { a3, b3, c3 };
		SpreadSheetCellDAO[] row3 = new SpreadSheetCellDAO[] { a4 };
		outputFormat.writeRecord(row1);
		outputFormat.writeRecord(row2);
		outputFormat.writeRecord(row3);
		outputFormat.close();
		// Read the file back
		FileInputSplit spreadSheetInputSplit = new FileInputSplit(0, new Path(file, "1"), 0, -1, null);
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setLocale(Locale.US);
		boolean useHeader = false;
		ExcelFlinkFileInputFormat inputFormat = new ExcelFlinkFileInputFormat(hocr, useHeader);
		inputFormat.open(spreadSheetInputSplit);
		assertFalse(inputFormat.reachedEnd(), "End not reached");
		SpreadSheetCellDAO[] reuse = new SpreadSheetCellDAO[0];
		SpreadSheetCellDAO[] nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "First Row returned");
		assertEquals(3, nextRow.length, "First row has 3 columns");
		assertEquals("test1", nextRow[0].getFormattedValue(), "1th Row 1th Cell formattedValue = \"test1\"");
		assertEquals("A1", nextRow[0].getAddress(), "1th Row 1th Cell address = \"A1\"");
		assertEquals("", nextRow[0].getComment(), "1th Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "1th Row 1th cell Sheet = \"Sheet1\"");
		assertEquals("test2", nextRow[1].getFormattedValue(), "1th Row 2nd Cell formattedValue = \"test2\"");
		assertEquals("B1", nextRow[1].getAddress(), "1th Row 2nd Cell address = \"B1\"");
		assertEquals("", nextRow[1].getComment(), "1th Row 2nd Comment = \"\"");
		assertEquals("Sheet1", nextRow[1].getSheetName(), "1th Row 2nd Cell Sheet = \"Sheet1\"");
		assertEquals("test3", nextRow[2].getFormattedValue(), "1th Row 3rd Cell formattedValue = \"test3\"");
		assertEquals("C1", nextRow[2].getAddress(), "1th Row 3rd Celll address = \"C1\"");
		assertEquals("", nextRow[2].getComment(), "1th Row 3rd Cell Comment = \"\"");
		assertEquals("Sheet1", nextRow[2].getSheetName(), "1th Row 3rd Cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Second Row returned");
		assertEquals(0, nextRow.length, "Second row is empty");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Third Row returned");
		assertEquals(3, nextRow.length, "Third row has 3 columns");
		assertEquals("1", nextRow[0].getFormattedValue(), "3rd Row 1th Cell formattedValue = \"1\"");
		assertEquals("A3", nextRow[0].getAddress(), "3rd Row 1th Cell address = \"A3\"");
		assertEquals("", nextRow[0].getComment(), "3rd Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "3rd Row 1th cell Sheet = \"Sheet1\"");
		assertEquals("2", nextRow[1].getFormattedValue(), "3rd Row 2nd Cell formattedValue = \"2\"");
		assertEquals("B3", nextRow[1].getAddress(), "3rd Row 2nd Cell address = \"B3\"");
		assertEquals("", nextRow[1].getComment(), "3rd Row 2nd Comment = \"\"");
		assertEquals("Sheet1", nextRow[1].getSheetName(), "3rd Row 2nd Cell Sheet = \"Sheet1\"");
		assertEquals("3", nextRow[2].getFormattedValue(), "3rd Row 3rd Cell formattedValue = \"3\"");
		assertEquals("C3", nextRow[2].getAddress(), "3rd Row 3rd Celll address = \"C3\"");
		assertEquals("", nextRow[2].getComment(), "3rd Row 3rd Cell Comment = \"\"");
		assertEquals("Sheet1", nextRow[2].getSheetName(), "3rd Row 3rd Cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Forth Row returned");
		assertEquals(1, nextRow.length, "Forth row has 1 columns");
		assertEquals("3", nextRow[0].getFormattedValue(), "4th Row 1th Cell formattedValue = \"3\"");
		assertEquals("A4", nextRow[0].getAddress(), "4th Row 1th Cell address = \"A4\"");
		assertEquals("", nextRow[0].getComment(), "4th Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "4th Row 1th cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertTrue(inputFormat.reachedEnd(), "End reached");
		inputFormat.close();
	}
	
	@Test
	public void writeExcel2013SingleSheetWithHeader() throws IOException {
		// write
		String fileName = "excel2013singlesheetoutwithhead.xls";
		Path file = new Path(this.tmpPath.toString(), fileName);
		HadoopOfficeWriteConfiguration howc = new HadoopOfficeWriteConfiguration("1");
		howc.setMimeType(FlinkExcelFileOutputFormatTest.MIMETYPE_XLSX);
		String[] header = new String[] {"test1","test2","test3"};
		String defaultSheetName = "Sheet1";
		ExcelFlinkFileOutputFormat outputFormat = new ExcelFlinkFileOutputFormat(howc, header, defaultSheetName);
		outputFormat.setOutputFilePath(file);
		outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
		outputFormat.initializeGlobal(1);

		outputFormat.open(0, 2);
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		// empty row => nothing todo
		// one row numbers (1,2,3)
		SpreadSheetCellDAO a3 = new SpreadSheetCellDAO("", "", "1", "A3", "Sheet1");
		SpreadSheetCellDAO b3 = new SpreadSheetCellDAO("", "", "2", "B3", "Sheet1");
		SpreadSheetCellDAO c3 = new SpreadSheetCellDAO("", "", "3", "C3", "Sheet1");
		// one row formulas (=A3+B3)
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("", "", "A3+B3", "A4", "Sheet1");
		SpreadSheetCellDAO[] row2 = new SpreadSheetCellDAO[] { a3, b3, c3 };
		SpreadSheetCellDAO[] row3 = new SpreadSheetCellDAO[] { a4 };
		outputFormat.writeRecord(row2);
		outputFormat.writeRecord(row3);
		outputFormat.close();
		// Read the file back
		FileInputSplit spreadSheetInputSplit = new FileInputSplit(0, new Path(file, "1"), 0, -1, null);
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		hocr.setLocale(Locale.US);
		boolean useHeader = true;
		ExcelFlinkFileInputFormat inputFormat = new ExcelFlinkFileInputFormat(hocr, useHeader);
		inputFormat.open(spreadSheetInputSplit);
		assertFalse(inputFormat.reachedEnd(), "End not reached");
		String[] expectedHeader = new String[] {"test1","test2","test3"};
		assertArrayEquals(expectedHeader,inputFormat.getHeader(),"Header is correctly read");
		SpreadSheetCellDAO[] reuse = new SpreadSheetCellDAO[0];
		SpreadSheetCellDAO[] nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Second Row returned");
		assertEquals(0, nextRow.length, "Second row is empty");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Third Row returned");
		assertEquals(3, nextRow.length, "Third row has 3 columns");
		assertEquals("1", nextRow[0].getFormattedValue(), "3rd Row 1th Cell formattedValue = \"1\"");
		assertEquals("A3", nextRow[0].getAddress(), "3rd Row 1th Cell address = \"A3\"");
		assertEquals("", nextRow[0].getComment(), "3rd Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "3rd Row 1th cell Sheet = \"Sheet1\"");
		assertEquals("2", nextRow[1].getFormattedValue(), "3rd Row 2nd Cell formattedValue = \"2\"");
		assertEquals("B3", nextRow[1].getAddress(), "3rd Row 2nd Cell address = \"B3\"");
		assertEquals("", nextRow[1].getComment(), "3rd Row 2nd Comment = \"\"");
		assertEquals("Sheet1", nextRow[1].getSheetName(), "3rd Row 2nd Cell Sheet = \"Sheet1\"");
		assertEquals("3", nextRow[2].getFormattedValue(), "3rd Row 3rd Cell formattedValue = \"3\"");
		assertEquals("C3", nextRow[2].getAddress(), "3rd Row 3rd Celll address = \"C3\"");
		assertEquals("", nextRow[2].getComment(), "3rd Row 3rd Cell Comment = \"\"");
		assertEquals("Sheet1", nextRow[2].getSheetName(), "3rd Row 3rd Cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertNotNull(nextRow, "Forth Row returned");
		assertEquals(1, nextRow.length, "Forth row has 1 columns");
		assertEquals("3", nextRow[0].getFormattedValue(), "4th Row 1th Cell formattedValue = \"3\"");
		assertEquals("A4", nextRow[0].getAddress(), "4th Row 1th Cell address = \"A4\"");
		assertEquals("", nextRow[0].getComment(), "4th Row 1th Comment = \"\"");
		assertEquals("Sheet1", nextRow[0].getSheetName(), "4th Row 1th cell Sheet = \"Sheet1\"");
		nextRow = inputFormat.nextRecord(reuse);
		assertTrue(inputFormat.reachedEnd(), "End reached");
		inputFormat.close();
	}
}

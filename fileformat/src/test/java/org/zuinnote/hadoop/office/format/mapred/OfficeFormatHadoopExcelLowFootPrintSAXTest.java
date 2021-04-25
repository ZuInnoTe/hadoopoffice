/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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

package org.zuinnote.hadoop.office.format.mapred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

public class OfficeFormatHadoopExcelLowFootPrintSAXTest {
	private static JobConf defaultConf = new JobConf();
	private static FileSystem localFs = null;
	private static Reporter reporter = Reporter.NULL;
	private static final String attempt = "attempt_201612311111_0001_m_000000_0";
	private static final String tmpPrefix = "hadoopofficetest";
	private static java.nio.file.Path tmpPath;

	@BeforeAll
	public static void oneTimeSetUp() throws IOException {
		// one-time initialization code
		defaultConf.set("fs.defaultFS", "file:///");
		localFs = FileSystem.getLocal(defaultConf);
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
		// workaround for Apache POI 4.0
		System.setProperty("org.apache.xml.security.ignoreLineBreaks", "true");
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
	public void readExcelInputFormatExcel2013MultiSheetHeaderLowFootPrint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "multisheetheader.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "us");
		job.set("hadoopoffice.read.header.read", "true");
		job.set("hadoopoffice.read.header.skipheaderinallsheets", "true");
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");

		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");

		assertEquals("column1", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[0],
				" header column 1 correctly read");
		assertEquals("column2", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[1],
				" header column 2 correctly read");
		assertEquals("column3", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[2],
				" header column 3 correctly read");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// First Sheet
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A2 = \"1\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B2 = \"test1\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C2 = \"10\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A3 = \"2\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B3 = \"test3\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B3", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C3 = \"15\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A4 = \"10\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B4 = \"test2\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("20", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C4 = \"20\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C4", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");
		// Second Sheet

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 1 (second sheet)");
		assertEquals("50", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A2 = \"50\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B2 = \"test1\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("80", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C2 = \"80\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 2 (second sheet)");
		assertEquals("60", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A3 = \"60\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B3 = \"test3\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B3", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("90", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C3 = \"90\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 3 (second sheet)");
		assertEquals("70", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A4 = \"70\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B4 = \"test2\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("10000", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C4 = \"10000\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C4", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");
		assertFalse(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains no further row");
	}


	@Test
	public void readExcelInputFormatExcel2013MultiSheetHeaderRegExLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "multisheetheader.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "us");
		job.set("hadoopoffice.read.header.read", "true");
		job.set("hadoopoffice.read.header.skipheaderinallsheets", "true");
		job.set("hadoopoffice.read.header.column.names.regex", "column");
		job.set("hadoopoffice.read.header.column.names.replace", "spalte");
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");

		assertEquals("spalte1", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[0],
				" header column 1 correctly read");
		assertEquals("spalte2", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[1],
				" header column 2 correctly read");
		assertEquals("spalte3", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[2],
				" header column 3 correctly read");
	}

	@Test
	public void readExcelInputFormatExcel2013MultiSheetSkipWithHeaderLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "skipsheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "us");
		job.set("hadoopoffice.read.header.read", "true");
		job.set("hadoopoffice.read.header.skipheaderinallsheets", "true");
		job.set("hadoopoffice.read.sheet.skiplines.num", "5");
		job.set("hadoopoffice.read.sheet.skiplines.allsheets", "true");
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		assertEquals("column1", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[0],
				" header column 1 correctly read");
		assertEquals("column2", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[1],
				" header column 2 correctly read");
		assertEquals("column3", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[2],
				" header column 3 correctly read");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// First Sheet
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("20", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A7 = \"20\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A7", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B7 = \"test2\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B7", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C7 = \"5\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C7", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals("30", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A8 = \"30\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B8 = \"test1\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B8", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C8 = \"10\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C8", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals("40", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A9 = \"40\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A9", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B9 = \"test3\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B9", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C9 = \"15\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C9", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");
		// Second Sheet

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 1 (second sheet)");
		assertEquals("90", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A7 = \"90\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A7", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B7 = \"test2\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B7", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("230", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C7 = \"230\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C7", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 2 (second sheet)");
		assertEquals("200", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A8 = \"200\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B8 = \"test1\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B8", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("240", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C8 = \"240\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C8", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 3 (second sheet)");
		assertEquals("101", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(), "A9 = \"101\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormula(), "Empty formula");
		assertEquals("A9", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(), "Correct sheet");

		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(), "B9 = \"test3\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormula(), "Empty formula");
		assertEquals("B9", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getSheetName(), "Correct sheet");

		assertEquals("250", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(), "C9 = \"250\"");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(), "Empty Comment");
		assertEquals("", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormula(), "Empty formula");
		assertEquals("C9", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getAddress(), "Correct address");
		assertEquals("Sheet2", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getSheetName(), "Correct sheet");
		// third sheet should not be read because all the lines are skipped
		assertFalse(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains no further row");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetLowFootPrintSignedPositive() throws IOException {
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// empty row => nothing todo
		// one row numbers (1,2,3)
		SpreadSheetCellDAO a3 = new SpreadSheetCellDAO("1", "", "1", "A3", "Sheet1");
		SpreadSheetCellDAO b3 = new SpreadSheetCellDAO("2", "", "2", "B3", "Sheet1");
		SpreadSheetCellDAO c3 = new SpreadSheetCellDAO("3", "", "3", "C3", "Sheet1");
		// one row formulas (=A3+B3)
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("", "", "A3+B3", "A4", "Sheet1");
		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutlowfootprintsignedpositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		/// signature
		String pkFileName = "testsigning.pfx"; // private key
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameKeyStore = classLoader.getResource(pkFileName).getFile();

		job.set("hadoopoffice.write.security.sign.keystore.file", fileNameKeyStore);
		job.set("hadoopoffice.write.security.sign.keystore.type", "PKCS12");
		job.set("hadoopoffice.write.security.sign.keystore.password", "changeit");
		job.set("hadoopoffice.write.security.sign.keystore.alias", "testalias");
		job.set("hadoopoffice.write.security.sign.hash.algorithm", "sha512");
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		job.set("hadoopoffice.read.security.sign.verifysignature", "true");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetLowFootPrintSignedNegative() throws IOException {
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
		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutlowfootprintsignednegative";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes

		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		job.set("hadoopoffice.read.security.sign.verifysignature", "true"); // will fail no signature
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNull(reader, "Format returned  null RecordReader, because no signature present");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetLowFootPrintSignedPositiveReadLowFootprint()
			throws IOException {
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");

		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutlowfootprintsignedpositivereadlowfootprint";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		/// signature
		String pkFileName = "testsigning.pfx"; // private key
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameKeyStore = classLoader.getResource(pkFileName).getFile();

		job.set("hadoopoffice.write.security.sign.keystore.file", fileNameKeyStore);
		job.set("hadoopoffice.write.security.sign.keystore.type", "PKCS12");
		job.set("hadoopoffice.write.security.sign.keystore.password", "changeit");
		job.set("hadoopoffice.write.security.sign.keystore.alias", "testalias");
		job.set("hadoopoffice.write.security.sign.hash.algorithm", "sha512");
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint

		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		job.set("hadoopoffice.read.security.sign.verifysignature", "true");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");

	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetLowFootPrintSignedNegativeReadLowFootprint()
			throws IOException {
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");

		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutlowfootprintsignednegativereadlowfootprint";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint

		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		job.set("hadoopoffice.read.security.sign.verifysignature", "true"); // will fail no signature provided
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNull(reader, "Format returned  null RecordReader, because no signature present");

	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedPositiveLowFootprintSignedPositive()
			throws IOException {
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
		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutencryptedpositivelowfootprintsignedpositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		job.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		job.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		job.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		job.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		job.set("hadoopoffice.write.security.crypt.password", "test");
		/// signature
		String pkFileName = "testsigning.pfx"; // private key
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameKeyStore = classLoader.getResource(pkFileName).getFile();

		job.set("hadoopoffice.write.security.sign.keystore.file", fileNameKeyStore);
		job.set("hadoopoffice.write.security.sign.keystore.type", "PKCS12");
		job.set("hadoopoffice.write.security.sign.keystore.password", "changeit");
		job.set("hadoopoffice.write.security.sign.keystore.alias", "testalias");
		job.set("hadoopoffice.write.security.sign.hash.algorithm", "sha512");
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		job.set("hadoopoffice.read.security.crypt.password", "test");
		job.set("hadoopoffice.read.security.sign.verifysignature", "true");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedPositiveLowFootprintSignedNegative()
			throws IOException {
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
		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutencryptedpositivelowfootprintsignednegative";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		job.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		job.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		job.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		job.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		job.set("hadoopoffice.write.security.crypt.password", "test");

		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		job.set("hadoopoffice.read.security.crypt.password", "test");
		job.set("hadoopoffice.read.security.sign.verifysignature", "true"); // will fail no signature present
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNull(reader, "Format returned  null RecordReader, because no signature present");
	}

	@Test
	public void readExcelInputFormatExcel2003SingleSheetLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003test.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[excel2003test.xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2003test.xls]Sheet1!A1\"");
		assertEquals(4, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 4 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue.get()[3]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 5");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 6");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
	}

	@Test
	public void readExcelInputFormatExcel2003MultiSheetAllLowFootPrint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003testmultisheet.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 1 (first sheet)");
		assertEquals("[excel2003testmultisheet.xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2093testmultisheet.xls]Sheet1!A1\"");
		assertEquals(4, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 4 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue.get()[3]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 2 (first sheet)");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 3 (first sheet)");
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 4 (first sheet)");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 5 (first sheet)");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 6 (first sheet)");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 7 (second sheet)");
		assertEquals("8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 1 == \"8\"");
		assertEquals("99", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 2 == \"99\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 7 with 2 columns");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 8 (second sheet)");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 8 with 1 column");
		assertEquals("test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 8 with cell 1 == \"test\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 9 (second sheet)");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 9 with 3 columns");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 9 with cell 1 == null");
		assertNull(spreadSheetValue.get()[1], "Input Split for Excel file contains row 9 with cell 2 == null");
		assertEquals("seven", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 9 with cell 3 == \"seven\"");
	}

	@Test
	public void readExcelInputFormatExcel2013SingleSheetLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[excel2013test.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013test.xlsx]Sheet1!A1\"");
		assertEquals(4, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 4 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue.get()[3]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 5");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 6");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetLowFootPrint() throws IOException {
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
		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutlowfootprint";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedPositiveLowFootprint() throws IOException {
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
		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutencryptedpositivelowfootprint";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		job.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		job.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		job.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		job.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		job.set("hadoopoffice.write.security.crypt.password", "test");
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		job.set("hadoopoffice.read.security.crypt.password", "test");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedNegativeLowFootprint() throws IOException {
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
		// write
		JobConf job = new JobConf(defaultConf);
		String fileName = "excel2013singlesheettestoutencryptednegativelowfoodprint";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		// set generic outputformat settings
		job.set(JobContext.TASK_ATTEMPT_ID, attempt);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.write.lowfootprint", "true");
		job.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		job.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		job.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		job.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		job.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		job.set("hadoopoffice.write.security.crypt.password", "test");
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(null, job, fileName, null);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(reporter);
		// try to read it again
		job = new JobConf(defaultConf);
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ "_temporary" + File.separator + attempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		job.set("hadoopoffice.read.security.crypt.password", "test2");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		inputFormat.configure(job);
		InputSplit[] inputSplits = inputFormat.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.getRecordReader(inputSplits[0], job, reporter);
		assertNull(reader, "Null record reader implies invalid password");
	}

	@Test
	public void readExcelInputFormatExcel2013MultiSheetAllLowFootPrint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 1 (first sheet)");
		assertEquals("[excel2013testmultisheet.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013testmultisheet.xlsx]Sheet1!A1\"");
		assertEquals(4, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 4 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue.get()[3]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 2 (first sheet)");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 3 (first sheet)");
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 4 (first sheet)");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 5 (first sheet)");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 6 (first sheet)");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 7 (second sheet)");
		assertEquals("8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 1 == \"8\"");
		assertEquals("99", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 2 == \"99\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 7 with 2 columns");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 8 (second sheet)");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 8 with 1 column");
		assertEquals("test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 8 with cell 1 == \"test\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue),
				"Input Split for Excel file contains row 9 (second sheet)");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 9 with 3 columns");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 9 with cell 1 == null");
		assertNull(spreadSheetValue.get()[1], "Input Split for Excel file contains row 9 with cell 2 == null");
		assertEquals("seven", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 9 with cell 3 == \"seven\"");
	}

	@Test
	public void readExcelInputFormatExcel2003SingleSheetEncryptedPositiveLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003encrypt.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");
		// for decryption simply set the password
		job.set("hadoopoffice.read.security.crypt.password", "test");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[excel2003encrypt.xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2003encrypt.xls]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
	}

	@Test
	public void readExcelInputFormatExcel2013SingleSheetEncryptedPositiveLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		// for decryption simply set the password
		job.set("hadoopoffice.read.security.crypt.password", "test");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[excel2013encrypt.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013encrypt.xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
	}

	@Test
	public void readExcelInputFormatExcel2013SingleSheetEncryptedNegativeLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		// for decryption simply set the password

		job.set("hadoopoffice.read.security.crypt.password", "test2");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNull(reader, "Null record reader implies invalid password");
	}


	@Test
	public void readExcelInputFormatExcel2003SingleSheetEncryptedNegativeLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003encrypt.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");
		// for decryption simply set the password
		job.set("hadoopoffice.read.security.crypt.password", "test2");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNull(reader, "Null record reader implies invalid password");
	}

	@Test
	public void readExcelInputFormatExcel2013EmptyRowsLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testemptyrows.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");

		job.set("hadoopoffice.read.lowfootprint.parser", "sax");
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[excel2013testemptyrows.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013testemptyrows.xlsx]Sheet1!A1\"");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 0 columns");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 2 columns");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 0 columns");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 5");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 6");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"10\"");
	}

	@Test
	public void readExcelInputFormatExcel2003EmptyRowsLowFootprint() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003testemptyrows.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowfootprint", "true");

		ExcelFileInputFormat format = new ExcelFileInputFormat();
		format.configure(job);
		InputSplit[] inputSplits = format.getSplits(job, 1);
		assertEquals(1, inputSplits.length, "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
		assertNotNull(reader, "Format returned  null RecordReader");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 1");
		assertEquals("[excel2003testemptyrows.xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2003testemptyrows.xls]Sheet1!A1\"");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 0 columns");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 2");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 2 columns");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 3");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 0 columns");

		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 4");
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 5");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.next(spreadSheetKey, spreadSheetValue), "Input Split for Excel file contains row 6");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"10\"");
	}

}

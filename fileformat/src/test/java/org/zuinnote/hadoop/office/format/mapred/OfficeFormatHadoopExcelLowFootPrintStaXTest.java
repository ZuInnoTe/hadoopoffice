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
package org.zuinnote.hadoop.office.format.mapred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

public class OfficeFormatHadoopExcelLowFootPrintStaXTest {
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootprintStax() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowFootprint", "true");
		// stax parser
		job.set("hadoopoffice.read.lowFootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootprintStaxPartlyInMemoryCompressed() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowFootprint", "true");
		// stax parser
		job.set("hadoopoffice.read.lowFootprint.parser", "stax");
		// partly in  memory compressed
		job.set("hadoopoffice.read.lowFootprint.stax.sst.cache", "1");
		job.set("hadoopoffice.read.lowFootprint.stax.sst.compress", "true");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootprintStaxPartlyInMemory() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowFootprint", "true");
		// stax parser
		job.set("hadoopoffice.read.lowFootprint.parser", "stax");
		// partly in memory
		job.set("hadoopoffice.read.lowFootprint.stax.sst.cache", "2");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootprintStaxNothingInMemory() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowFootprint", "true");
		// stax parser
		job.set("hadoopoffice.read.lowFootprint.parser", "stax");
		// nothing memory
		job.set("hadoopoffice.read.lowFootprint.stax.sst.cache", "0");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootprintStaxAllMemory() throws IOException {
		JobConf job = new JobConf(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		FileInputFormat.setInputPaths(job, file);
		// set locale to the one of the test data
		job.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		job.set("hadoopoffice.read.lowFootprint", "true");
		// stax parser
		job.set("hadoopoffice.read.lowFootprint.parser", "stax");
		// all memory
		job.set("hadoopoffice.read.lowFootprint.stax.sst.cache", "-1");
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

}

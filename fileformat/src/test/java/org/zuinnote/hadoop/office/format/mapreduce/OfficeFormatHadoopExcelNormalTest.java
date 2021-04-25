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

package org.zuinnote.hadoop.office.format.mapreduce;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.apache.hadoop.util.ReflectionUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.dao.*;

public class OfficeFormatHadoopExcelNormalTest {

	private static Configuration defaultConf = new Configuration();
	private static FileSystem localFs = null;
	private static final String attempt = "attempt_201612311111_0001_m_000000_0";
	private static final String taskAttempt = "task_201612311111_0001_m_000000";
	private static final TaskAttemptID taskID = TaskAttemptID.forName(attempt);
	private static final String tmpPrefix = "hadoopofficetest";
	private static final String outputbaseAppendix = "-m-00000";
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
	public void readExcelInputFormatExcel2003Empty() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003empty.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains no row");
	}

	@Test
	public void readExcelInputFormatExcel2013Empty() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013empty.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains no row");
	}

	@Test
	public void readExcelInputFormatExcel2003SingleSheet() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003test.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 5");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 6");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
	}

	@Test
	public void readExcelInputFormatExcel2013SingleSheetEncryptedPositive() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// for decryption simply set the password
		conf.set("hadoopoffice.read.security.crypt.password", "test");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
	public void readExcelInputFormatExcel2013SingleSheetEncryptedNegative() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// for decryption simply set the password
		conf.set("hadoopoffice.read.security.crypt.password", "test2");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);

		InterruptedException ex = assertThrows(InterruptedException.class,
				() -> reader.initialize(splits.get(0), context), "Exception is thrown in case of wrong password");
	}

	@Test
	public void readExcelInputFormatExcel2003SingleSheetEncryptedPositive() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003encrypt.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// for decryption simply set the password
		conf.set("hadoopoffice.read.security.crypt.password", "test");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
	public void readExcelInputFormatExcel2003SingleSheetEncryptedNegative() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003encrypt.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// for decryption simply set the password
		conf.set("hadoopoffice.read.security.crypt.password", "test2");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		InterruptedException ex = assertThrows(InterruptedException.class,
				() -> reader.initialize(splits.get(0), context), "Exception is thrown in case of wrong password");
	}

	@Test
	public void readExcelInputFormatExcel2013EmptyRows() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testemptyrows.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2013testemptyrows.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013testemptyrows.xlsx]Sheet1!A1\"");
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 0 columns");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 2 columns");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 0 columns");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 5");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 6");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"10\"");
	}

	@Test
	public void readExcelInputFormatExcel2013SingleSheet() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 5");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 6");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
	}

	@Test
	public void readExcelCellInputFormatExcel2013SingleSheet() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelCellFileInputFormat format = new ExcelCellFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, SpreadSheetCellDAO> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		SpreadSheetCellDAO spreadSheetValue = new SpreadSheetCellDAO();
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2013test.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013test.xlsx]Sheet1!A1\"");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("Sheet1", ((SpreadSheetCellDAO) spreadSheetValue).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 4 == \"test4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains cell");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
		assertFalse(reader.nextKeyValue(), "No more cells");
	}

	@Test
	public void readExcelInputFormatExcel2013MultiSheetAll() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 5 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 6 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 7 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 1 == \"8\"");
		assertEquals("99", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 2 == \"99\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 7 with 2 columns");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 8 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 8 with 1 column");
		assertEquals("test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 8 with cell 1 == \"test\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 9 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 9 with 3 columns");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 9 with cell 1 == null");
		assertNull(spreadSheetValue.get()[1], "Input Split for Excel file contains row 9 with cell 2 == null");
		assertEquals("seven", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 9 with cell 3 == \"seven\"");
	}

	@Test
	public void readExcelInputFormatExcel2013MultiSheetSelectedSheet() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// select the sheet
		conf.set("hadoopoffice.read.sheets", "testsheet");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 7 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 1 == \"8\"");
		assertEquals("99", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 2 == \"99\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 7 with 2 columns");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 8 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 8 with 1 column");
		assertEquals("test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 8 with cell 1 == \"test\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 9 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 9 with 3 columns");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 9 with cell 1 == null");
		assertNull(spreadSheetValue.get()[1], "Input Split for Excel file contains row 9 with cell 2 == null");
		assertEquals("seven", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 9 with cell 3 == \"seven\"");
	}

	@Test
	public void readExcelInputFormatExcel2013Comment() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013comment.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Path file = new Path(fileNameSpreadSheet);
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2013comment.xlsx]CommentSheet!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013comment.xlsx]CommentSheet!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("First comment", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getComment(),
				"Input Split for Excel file contains row 1 with cell 1 comment == \"First comment\"");
		assertEquals("CommentSheet", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getSheetName(),
				"Input Split for Excel file contains row 1 with cell 1 sheetname == \"CommentSheet\"");
		assertEquals("A1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getAddress(),
				"Input Split for Excel file contains row 1 with cell 1 address == \"A1\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test3\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 2 columns");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 3 with cell 1 == null");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 2 == \"5\"");
		assertEquals("Second comment", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(),
				"Input Split for Excel file contains row 2 with cell 2 comment == \"Second comment\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 3 column");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 4 with cell 1 == null");
		assertNull(spreadSheetValue.get()[1], "Input Split for Excel file contains row 4 with cell 2 == null");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 3 == \"6\"");
		assertEquals("Third comment", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getComment(),
				"Input Split for Excel file contains row 4 with cell 3 comment == \"Third comment\"");
	}

	@Test
	public void readExcelInputFormatGzipCompressedExcel2013MultiSheetAll() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		CompressionCodec gzip = new GzipCodec();
		ReflectionUtils.setConf(gzip, conf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx.gz";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2013testmultisheet.xlsx.gz]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013testmultisheet.xlsx.gz]Sheet1!A1\"");
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 5 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 6 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 7 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 1 == \"8\"");
		assertEquals("99", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 2 == \"99\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 7 with 2 columns");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 8 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 8 with 1 column");
		assertEquals("test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 8 with cell 1 == \"test\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 9 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 9 with 3 columns");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 9 with cell 1 == null");
		assertNull(spreadSheetValue.get()[1], "Input Split for Excel file contains row 9 with cell 2 == null");
		assertEquals("seven", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 9 with cell 3 == \"seven\"");
	}

	@Test
	public void readExcelInputFormatExcel2013LinkedWorkbook() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbooks.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2013linkedworkbooks.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013linkedworkbooks.xlsx]Sheet1!A1\"");
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 2 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"3\" (this tests also if the cached value of 6 is ignored)");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"5\"");

	}

	@Test
	public void readExcelInputFormatExcel2013LinkedWorkbookAlternativeLocation() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbooks.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// alternativeLocation
		String resourcePath = new File(classLoader.getResource(fileName).getFile()).getParent();
		String alternativeLocation = resourcePath+File.separator+"alternatelocationlinkedwb";
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);
		conf.set("hadoopoffice.read.linkedworkbooks.location", alternativeLocation);
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2013linkedworkbooks.xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013linkedworkbooks.xlsx]Sheet1!A1\"");
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 2 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"3\" (this tests also if the cached value of 6 is ignored)");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"5\"");

	}

	@Test
	public void readExcelInputFormatExcel2003LinkedWorkbook() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003linkedworkbooks.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);
		Job job = Job.getInstance(conf);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		FileInputFormat.setInputPaths(job, file);
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2003linkedworkbooks.xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2003linkedworkbooks.xls]Sheet1!A1\"");
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 2 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"3\" (this tests also if the cached value of 6 is ignored)");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"5\"");

	}

	@Test
	public void readExcelInputFormatExcel2003LinkedWorkbookAlternativeLocation() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003linkedworkbooks.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// alternativeLocation
		String resourcePath = new File(classLoader.getResource(fileName).getFile()).getParent();
		String alternativeLocation = resourcePath+File.separator+"alternatelocationlinkedwb";
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);

		conf.set("hadoopoffice.read.linkedworkbooks.location", alternativeLocation);
		Job job = Job.getInstance(conf);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		FileInputFormat.setInputPaths(job, file);
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2003linkedworkbooks.xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2003linkedworkbooks.xls]Sheet1!A1\"");
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 2 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"3\" (this tests also if the cached value of 6 is ignored)");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"5\"");

	}

	@Test
	public void readExcelInputFormatBzip2CompressedExcel2013MultiSheetAll() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		CompressionCodec bzip2 = new BZip2Codec();
		ReflectionUtils.setConf(bzip2, conf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx.bz2";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[excel2013testmultisheet.xlsx.bz2]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[excel2013testmultisheet.xlsx.bz2]Sheet1!A1\"");
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 1 column");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"4\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(5, spreadSheetValue.get().length, "Input Split for Excel file contains row 3 with 5 columns");
		assertEquals("31/12/99", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"");
		assertEquals("5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"5\"");
		assertNull(spreadSheetValue.get()[2], "Input Split for Excel file contains row 3 with cell 3 == null");
		assertNull(spreadSheetValue.get()[3], "Input Split for Excel file contains row 3 with cell 4 == null");
		assertEquals("null", ((SpreadSheetCellDAO) spreadSheetValue.get()[4]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 5 == \"null\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 4 with 1 column");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 4 with cell 1 == \"1\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 5 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 5 with 3 columns");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 1 == \"2\"");
		assertEquals("6", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 2== \"6\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 5 with cell 3== \"10\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 6 (first sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 6 with 3 columns");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 1 == \"3\"");
		assertEquals("4", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 2== \"4\"");
		assertEquals("15", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 6 with cell 3== \"15\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 7 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("8", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 1 == \"8\"");
		assertEquals("99", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 7 with cell 2 == \"99\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 7 with 2 columns");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 8 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contains row 8 with 1 column");
		assertEquals("test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 8 with cell 1 == \"test\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 9 (second sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 9 with 3 columns");
		assertNull(spreadSheetValue.get()[0], "Input Split for Excel file contains row 9 with cell 1 == null");
		assertNull(spreadSheetValue.get()[1], "Input Split for Excel file contains row 9 with cell 2 == null");
		assertEquals("seven", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 9 with cell 3 == \"seven\"");
	}

	@Test
	public void readExcelInputFormatExcel2013MultiSheetHeader() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "multisheetheader.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.locale.bcp47", "us");
		conf.set("hadoopoffice.read.header.read", "true");
		conf.set("hadoopoffice.read.header.skipheaderinallsheets", "true");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		assertEquals("column1", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[0],
				" header column 1 correctly read");
		assertEquals("column2", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[1],
				" header column 2 correctly read");
		assertEquals("column3", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[2],
				" header column 3 correctly read");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();

		// First Sheet
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 (Second Sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2 (Second Sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3 (Second Sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains no further row");
	}

	@Test
	public void readExcelInputFormatExcel2013MultiSheetHeaderRegEx() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "multisheetheader.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "us");
		conf.set("hadoopoffice.read.header.read", "true");
		conf.set("hadoopoffice.read.header.skipheaderinallsheets", "true");
		conf.set("hadoopoffice.read.header.column.names.regex","column");
		conf.set("hadoopoffice.read.header.column.names.replace", "spalte");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);

		assertEquals("spalte1", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[0],
				" header column 1 correctly read");
		assertEquals("spalte2", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[1],
				" header column 2 correctly read");
		assertEquals("spalte3", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[2],
				" header column 3 correctly read");
	}



	@Test
	public void readExcelInputFormatExcel2013MultiSheetSkipWithHeader() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "skipsheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.locale.bcp47", "us");
		conf.set("hadoopoffice.read.header.read", "true");
		conf.set("hadoopoffice.read.header.skipheaderinallsheets", "true");
		conf.set("hadoopoffice.read.sheet.skiplines.num", "5");
		conf.set("hadoopoffice.read.sheet.skiplines.allsheets", "true");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		assertEquals("column1", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[0],
				" header column 1 correctly read");
		assertEquals("column2", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[1],
				" header column 2 correctly read");
		assertEquals("column3", ((ExcelRecordReader) reader).getOfficeReader().getCurrentParser().getHeader()[2],
				" header column 3 correctly read");
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		// First Sheet
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 (Second Sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2 (Second Sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3 (Second Sheet)");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
		// third sheet is skipped because it does not contain enough rows
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains no further row");
	}



	@Test
	public void writeExcelOutputFormatExcel2013SingleSheet() throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		// set generic outputformat settings
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedPositive() throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutencryptedpositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set generic outputformat settings
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		conf.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		conf.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		conf.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		conf.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		conf.set("hadoopoffice.write.security.crypt.password", "test");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		conf.set("hadoopoffice.read.security.crypt.password", "test");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedNegative() throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2013singlesheettestoutencryptednegative";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		conf.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		conf.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		conf.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		conf.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		conf.set("hadoopoffice.write.security.crypt.password", "test");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		conf.set("hadoopoffice.read.security.crypt.password", "test2");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		TaskAttemptContext context2 = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context2);
		InterruptedException ex = assertThrows(InterruptedException.class,
				() -> reader.initialize(splits.get(0), context2), "Exception is thrown in case of wrong password");
	}

	@Test
	public void writeExcelOutputFormatExcel2003SingleSheetEncryptedPositive() throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2003singlesheettestoutencryptedpositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel"); // old excel format
		// security
		// for the old Excel format you simply need to define only a password
		conf.set("hadoopoffice.write.security.crypt.password", "test");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.read.security.crypt.password", "test");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xls]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2003SingleSheetEncryptedNegative() throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2003singlesheettestoutencryptedpositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel"); // old excel format
		// security
		// for the old Excel format you simply need to define only a password
		conf.set("hadoopoffice.write.security.crypt.password", "test");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.read.security.crypt.password", "test2");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		TaskAttemptContext context2 = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context2);
		InterruptedException ex = assertThrows(InterruptedException.class,
				() -> reader.initialize(splits.get(0), context2), "Exception is thrown in case of wrong password");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetMetaDataMatchAllPositive()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2013singlesheetmetadatapositivetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		// set all the meta data including to custom properties
		conf.set("hadoopoffice.write.metadata.category", "dummycategory");
		conf.set("hadoopoffice.write.metadata.contentstatus", "dummycontentstatus");
		conf.set("hadoopoffice.write.metadata.contenttype", "dummycontenttype");
		conf.set("hadoopoffice.write.metadata.created", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.creator", "dummycreator");
		conf.set("hadoopoffice.write.metadata.description", "dummydescription");
		conf.set("hadoopoffice.write.metadata.identifier", "dummyidentifier");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.modified", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.revision", "2");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty1", "dummymycustomproperty1");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty2", "dummymycustomproperty2");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "true");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.category", "dummycategory");
		conf.set("hadoopoffice.read.filter.metadata.contentstatus", "dummycontentstatus");
		conf.set("hadoopoffice.read.filter.metadata.contenttype", "dummycontenttype");
		conf.set("hadoopoffice.read.filter.metadata.created", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.creator", "dummycreator");
		conf.set("hadoopoffice.read.filter.metadata.description", "dummydescription");
		conf.set("hadoopoffice.read.filter.metadata.identifier", "dummyidentifier");
		conf.set("hadoopoffice.read.filter.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.read.filter.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.modified", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.read.filter.metadata.revision", "2");
		conf.set("hadoopoffice.read.filter.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.read.filter.metadata.title", "dummytitle");
		conf.set("hadoopoffice.read.filter.metadata.custom.mycustomproperty1", "dummymycustomproperty1");
		conf.set("hhadoopoffice.read.filter.metadata.custom.mycustomproperty2", "dummymycustomproperty2");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion is not true that means the document has (wrongly) been
		// filtered out
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
	public void writeExcelOutputFormatExcel2003SingleSheetMetaDataMatchAllPositive()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2003singlesheetmetadatapositivetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel"); // old Excel format

		// set all the meta data
		conf.set("hadoopoffice.write.metadata.applicationname", "dummyapplicationname");
		conf.set("hadoopoffice.write.metadata.author", "dummyauthor");
		conf.set("hadoopoffice.write.metadata.charcount", "1");
		conf.set("hadoopoffice.write.metadata.comments", "dummycomments");
		conf.set("hadoopoffice.write.metadata.createdatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.edittime", "0");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastauthor", "dummylastauthor");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastsavedatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.pagecount", "1");
		conf.set("hadoopoffice.write.metadata.revnumber", "1");
		conf.set("hadoopoffice.write.metadata.security", "0");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		// conf.set("hadoopoffice.write.metadata.template","dummytemplate");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		// conf.set("hadoopoffice.write.metadata.wordcount","1");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "true");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.applicationname", "dummyapplicationname");
		conf.set("hadoopoffice.read.filter.metadata.metadata.author", "dummyauthor");
		conf.set("hadoopoffice.read.filter.metadata.metadata.charcount", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.comments", "dummycomments");
		conf.set("hadoopoffice.read.filter.metadata.metadata.createdatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.metadata.edittime", "0");
		conf.set("hadoopoffice.read.filter.metadata.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastauthor", "dummylastauthor");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastsavedatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.metadata.pagecount", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.revnumber", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.security", "0");
		conf.set("hadoopoffice.read.filter.metadata.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.read.filter.metadata.metadata.template", "dummytemplate");
		conf.set("hadoopoffice.read.filter.metadata.metadata.title", "dummytitle");
		conf.set("hadoopoffice.read.filter.metadata.metadata.wordcount", "1");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion is not true that means the document has (wrongly) been
		// filtered out
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xls]Sheet1!A1", spreadSheetKey.toString(),
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
	public void writeExcelOutputFormatExcel2013SingleSheetMetaDataMatchAllNegative()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheetmetadatanegativetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		// set all the meta data including to custom properties
		conf.set("hadoopoffice.write.metadata.category", "dummycategory");
		conf.set("hadoopoffice.write.metadata.contentstatus", "dummycontentstatus");
		conf.set("hadoopoffice.write.metadata.contenttype", "dummycontenttype");
		conf.set("hadoopoffice.write.metadata.created", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.creator", "dummycreator");
		conf.set("hadoopoffice.write.metadata.description", "dummydescription");
		conf.set("hadoopoffice.write.metadata.identifier", "dummyidentifier");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.modified", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.revision", "2");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty1", "dummymycustomproperty1");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty2", "dummymycustomproperty2");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "true");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.category", "no Category");
		conf.set("hadoopoffice.read.filter.metadata.contentstatus", "dummycontentstatus");
		conf.set("hadoopoffice.read.filter.metadata.contenttype", "dummycontenttype");
		conf.set("hadoopoffice.read.filter.metadata.created", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.creator", "dummycreator");
		conf.set("hadoopoffice.read.filter.metadata.description", "dummydescription");
		conf.set("hadoopoffice.read.filter.metadata.identifier", "dummyidentifier");
		conf.set("hadoopoffice.read.filter.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.read.filter.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.modified", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.read.filter.metadata.revision", "2");
		conf.set("hadoopoffice.read.filter.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.read.filter.metadata.title", "dummytitle");
		conf.set("hadoopoffice.read.filter.metadata.custom.mycustomproperty1", "dummymycustomproperty1");
		conf.set("hhadoopoffice.read.filter.metadata.custom.mycustomproperty2", "dummymycustomproperty2");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion is true that means the document has wrongly NOT been
		// filtered out
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
	}

	@Test
	public void writeExcelOutputFormatExcel2003SingleSheetMetaDataMatchAllNegative()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2003singlesheetmetadatanegativetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel"); // old Excel format

		// set all the meta data
		conf.set("hadoopoffice.write.metadata.applicationname", "dummyapplicationname");
		conf.set("hadoopoffice.write.metadata.author", "dummyauthor");
		conf.set("hadoopoffice.write.metadata.charcount", "1");
		conf.set("hadoopoffice.write.metadata.comments", "dummycomments");
		conf.set("hadoopoffice.write.metadata.createdatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.edittime", "0");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastauthor", "dummylastauthor");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastsavedatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.pagecount", "1");
		conf.set("hadoopoffice.write.metadata.revnumber", "1");
		conf.set("hadoopoffice.write.metadata.security", "0");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.write.metadata.template", "dummytemplate");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		conf.set("hadoopoffice.write.metadata.wordcount", "1");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "true");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.applicationname", "dummyapplicationname2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.author", "dummyauthor");
		conf.set("hadoopoffice.read.filter.metadata.metadata.charcount", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.comments", "dummycomments");
		conf.set("hadoopoffice.read.filter.metadata.metadata.createdatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.metadata.edittime", "0");
		conf.set("hadoopoffice.read.filter.metadata.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastauthor", "dummylastauthor");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastsavedatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.read.filter.metadata.metadata.pagecount", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.revnumber", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.security", "0");
		conf.set("hadoopoffice.read.filter.metadata.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.read.filter.metadata.metadata.template", "dummytemplate");
		conf.set("hadoopoffice.read.filter.metadata.metadata.title", "dummytitle");
		conf.set("hadoopoffice.read.filter.metadata.metadata.wordcount", "1");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion not true that means the document has (wrongly) NOT
		// been filtered out
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetMetaDataMatchOncePositive()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2013singlesheetmetadatapositiveoncetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		// set all the meta data including to custom properties
		conf.set("hadoopoffice.write.metadata.category", "dummycategory");
		conf.set("hadoopoffice.write.metadata.contentstatus", "dummycontentstatus");
		conf.set("hadoopoffice.write.metadata.contenttype", "dummycontenttype");
		conf.set("hadoopoffice.write.metadata.created", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.creator", "dummycreator");
		conf.set("hadoopoffice.write.metadata.description", "dummydescription");
		conf.set("hadoopoffice.write.metadata.identifier", "dummyidentifier");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.modified", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.revision", "2");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty1", "dummymycustomproperty1");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty2", "dummymycustomproperty2");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "false");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.category", "dummycategory");
		conf.set("hadoopoffice.read.filter.metadata.contentstatus", "dummycontentstatus2");
		conf.set("hadoopoffice.read.filter.metadata.contenttype", "dummycontenttype2");
		conf.set("hadoopoffice.read.filter.metadata.created", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.creator", "dummycreator2");
		conf.set("hadoopoffice.read.filter.metadata.description", "dummydescription2");
		conf.set("hadoopoffice.read.filter.metadata.identifier", "dummyidentifier2");
		conf.set("hadoopoffice.read.filter.metadata.keywords", "dummykeywords2");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser2");
		conf.set("hadoopoffice.read.filter.metadata.lastprinted", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.modified", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser2");
		conf.set("hadoopoffice.read.filter.metadata.revision", "3");
		conf.set("hadoopoffice.read.filter.metadata.subject", "dummysubject2");
		conf.set("hadoopoffice.read.filter.metadata.title", "dummytitle2");
		conf.set("hadoopoffice.read.filter.metadata.custom.mycustomproperty1", "dummymycustomproperty12");
		conf.set("hhadoopoffice.read.filter.metadata.custom.mycustomproperty2", "dummymycustomproperty22");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion is not true that means the document has (wrongly) been
		// filtered out
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
	public void writeExcelOutputFormatExcel2003SingleSheetMetaDataMatchOncePositive()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2003singlesheetmetadatapositiveoncetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel"); // old Excel format

		// set all the meta data
		conf.set("hadoopoffice.write.metadata.applicationname", "dummyapplicationname");
		conf.set("hadoopoffice.write.metadata.author", "dummyauthor");
		conf.set("hadoopoffice.write.metadata.charcount", "1");
		conf.set("hadoopoffice.write.metadata.comments", "dummycomments");
		conf.set("hadoopoffice.write.metadata.createdatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.edittime", "0");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastauthor", "dummylastauthor");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastsavedatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.pagecount", "1");
		conf.set("hadoopoffice.write.metadata.revnumber", "1");
		conf.set("hadoopoffice.write.metadata.security", "0");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.write.metadata.template", "dummytemplate");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		conf.set("hadoopoffice.write.metadata.wordcount", "1");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "false");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.applicationname", "dummyapplicationname");
		conf.set("hadoopoffice.read.filter.metadata.metadata.author", "dummyautho2r");
		conf.set("hadoopoffice.read.filter.metadata.metadata.charcount", "2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.comments", "dummycomments2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.createdatetime", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.metadata.edittime", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.keywords", "dummykeywords2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastauthor", "dummylastauthor2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastprinted", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastsavedatetime", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.metadata.pagecount", "2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.revnumber", "2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.security", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.subject", "dummysubject2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.template", "dummytemplate2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.title", "dummytitle2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.wordcount", "2");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion is not true that means the document has (wrongly) been
		// filtered out
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xls]Sheet1!A1", spreadSheetKey.toString(),
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
	public void writeExcelOutputFormatExcel2013SingleSheetMetaDataMatchOnceNegative()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2013singlesheetmetadatanativeoncetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		// set all the meta data including to custom properties
		conf.set("hadoopoffice.write.metadata.category", "dummycategory");
		conf.set("hadoopoffice.write.metadata.contentstatus", "dummycontentstatus");
		conf.set("hadoopoffice.write.metadata.contenttype", "dummycontenttype");
		conf.set("hadoopoffice.write.metadata.created", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.creator", "dummycreator");
		conf.set("hadoopoffice.write.metadata.description", "dummydescription");
		conf.set("hadoopoffice.write.metadata.identifier", "dummyidentifier");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.modified", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser");
		conf.set("hadoopoffice.write.metadata.revision", "2");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty1", "dummymycustomproperty1");
		conf.set("hadoopoffice.write.metadata.custom.mycustomproperty2", "dummymycustomproperty2");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "false");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.category", "dummycategory2");
		conf.set("hadoopoffice.read.filter.metadata.contentstatus", "dummycontentstatus2");
		conf.set("hadoopoffice.read.filter.metadata.contenttype", "dummycontenttype2");
		conf.set("hadoopoffice.read.filter.metadata.created", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.creator", "dummycreator2");
		conf.set("hadoopoffice.read.filter.metadata.description", "dummydescription2");
		conf.set("hadoopoffice.read.filter.metadata.identifier", "dummyidentifier2");
		conf.set("hadoopoffice.read.filter.metadata.keywords", "dummykeywords2");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser2");
		conf.set("hadoopoffice.read.filter.metadata.lastprinted", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.modified", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.lastmodifiedbyuser", "dummylastmodifiedbyuser2");
		conf.set("hadoopoffice.read.filter.metadata.revision", "3");
		conf.set("hadoopoffice.read.filter.metadata.subject", "dummysubject2");
		conf.set("hadoopoffice.read.filter.metadata.title", "dummytitle2");
		conf.set("hadoopoffice.read.filter.metadata.custom.mycustomproperty1", "dummymycustomproperty12");
		conf.set("hhadoopoffice.read.filter.metadata.custom.mycustomproperty2", "dummymycustomproperty22");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion is true that means the document has (wrongly) NOT been
		// filtered out
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
	}

	@Test
	public void writeExcelOutputFormatExcel2003SingleSheetMetaDataMatchOnceNegative()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// the idea here is to have some content although we only evaluate metadata
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2003singlesheetmetadatanegativeoncetestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel"); // old Excel format

		// set all the meta data
		conf.set("hadoopoffice.write.metadata.applicationname", "dummyapplicationname");
		conf.set("hadoopoffice.write.metadata.author", "dummyauthor");
		conf.set("hadoopoffice.write.metadata.charcount", "1");
		conf.set("hadoopoffice.write.metadata.comments", "dummycomments");
		conf.set("hadoopoffice.write.metadata.createdatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.edittime", "0");
		conf.set("hadoopoffice.write.metadata.keywords", "dummykeywords");
		conf.set("hadoopoffice.write.metadata.lastauthor", "dummylastauthor");
		conf.set("hadoopoffice.write.metadata.lastprinted", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.lastsavedatetime", "12:00:00 01.01.2016");
		conf.set("hadoopoffice.write.metadata.pagecount", "1");
		conf.set("hadoopoffice.write.metadata.revnumber", "1");
		conf.set("hadoopoffice.write.metadata.security", "0");
		conf.set("hadoopoffice.write.metadata.subject", "dummysubject");
		conf.set("hadoopoffice.write.metadata.template", "dummytemplate");
		conf.set("hadoopoffice.write.metadata.title", "dummytitle");
		conf.set("hadoopoffice.write.metadata.wordcount", "1");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// set metadata to match all
		conf.set("hadoopoffice.read.filter.metadata.matchAll", "false");
		// following filter
		conf.set("hadoopoffice.read.filter.metadata.applicationname", "dummyapplicationname2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.author", "dummyautho2r");
		conf.set("hadoopoffice.read.filter.metadata.metadata.charcount", "2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.comments", "dummycomments2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.createdatetime", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.metadata.edittime", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.keywords", "dummykeywords2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastauthor", "dummylastauthor2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastprinted", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.metadata.lastsavedatetime", "12:00:00 01.01.2017");
		conf.set("hadoopoffice.read.filter.metadata.metadata.pagecount", "2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.revnumber", "2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.security", "1");
		conf.set("hadoopoffice.read.filter.metadata.metadata.subject", "dummysubject2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.template", "dummytemplate2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.title", "dummytitle2");
		conf.set("hadoopoffice.read.filter.metadata.metadata.wordcount", "2");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		// if following assertion is true that means the document has (wrongly) NOT been
		// filtered out
		assertFalse(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetGZipCompressed() throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheetcompressedtestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
		conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx.gz");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx.gz]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx.gz]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetComment() throws IOException, InterruptedException {
		// 2nd cell with a comment
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "This is a test", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2013singlesheetcommenttestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("This is a test", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getComment(),
				"Input Split for Excel file contains row 1 with cell 2 comment == \"This is a test\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");

	}

	@Test
	public void writeExcelOutputFormatExcel2013MultiSheet() throws IOException, InterruptedException {
		// one sheet "Sheet1"
		// one row string and three columns ("test1","test2","test3")
		SpreadSheetCellDAO sheet1a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO sheet1b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO sheet1c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// one sheet "Sheet2"
		// one row string and three columns ("test4","test5","test6")
		SpreadSheetCellDAO sheet2a1 = new SpreadSheetCellDAO("test4", "", "", "A1", "Sheet2");
		SpreadSheetCellDAO sheet2b1 = new SpreadSheetCellDAO("test5", "", "", "B1", "Sheet2");
		SpreadSheetCellDAO sheet2c1 = new SpreadSheetCellDAO("test6", "", "", "C1", "Sheet2");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013multisheettestout";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, sheet1a1);
		writer.write(null, sheet1b1);
		writer.write(null, sheet1c1);
		writer.write(null, sheet2a1);
		writer.write(null, sheet2b1);
		writer.write(null, sheet2c1);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 Sheet1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length,
				"Input Split for Excel file contains row 1 with 3 columns for Sheet1");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 Sheet2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet2!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet2!A1\"");
		assertEquals(3, spreadSheetValue.get().length,
				"Input Split for Excel file contains row 1 with 3 columns for Sheet1");
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test4\"");
		assertEquals("test5", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test5\"");
		assertEquals("test6", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test6\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2003SingleSheetOneLinkedWorkbook() throws IOException, InterruptedException {
		// write linkedworkbook1
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO wb1a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO wb1b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO wb1c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String linkedWB1FileName = "excel2003linkedwb1";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", linkedWB1FileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, wb1a1);
		writer.write(null, wb1b1);
		writer.write(null, wb1c1);
		writer.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// write mainworkbook
		linkedWB1FileName = linkedWB1FileName + this.outputbaseAppendix;
		String linkedWorkbookFilename = "[" + tmpDir + File.separator + linkedWB1FileName + ".xls]";
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test4", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("", "", "[" + linkedWB1FileName + ".xls]Sheet1!B1", "B1",
				"Sheet1"); // should be test2 in the end
		// write
		job = Job.getInstance();
		conf = job.getConfiguration();
		String mainWBfileName = "excel2003singlesheetlinkedwbtestout";
		outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", mainWBfileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel");
		conf.set("hadoopoffice.write.linkedworkbooks", linkedWorkbookFilename);
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		jContext = new JobContextImpl(conf, taskID.getJobID());

		context = new TaskAttemptContextImpl(conf, taskID);
		committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writerMain = outputFormat.getRecordWriter(context);
		assertNotNull(writerMain, "Format returned  null RecordWriter");
		writerMain.write(null, a1);
		writerMain.write(null, b1);
		writerMain.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		mainWBfileName = mainWBfileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + mainWBfileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 Sheet1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + mainWBfileName + ".xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + mainWBfileName + ".xls]Sheet1!A1\"");
		assertEquals(2, spreadSheetValue.get().length,
				"Input Split for Excel file contains row 1 with 2 columns for Sheet1");
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test4\"");
		// this comes from the external workbook
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2003SingleSheetTwoLinkedWorkbooks()
			throws IOException, InterruptedException {
		// write linkedworkbook1
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO wb1a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO wb1b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO wb1c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String linkedWB1FileName = "excel2003linkedwb1b";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", linkedWB1FileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, wb1a1);
		writer.write(null, wb1b1);
		writer.write(null, wb1c1);
		writer.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// write linkedworkbook2
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO wb2a1 = new SpreadSheetCellDAO("test4", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO wb2b1 = new SpreadSheetCellDAO("test5", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO wb2c1 = new SpreadSheetCellDAO("test6", "", "", "C1", "Sheet1");
		// write
		job = Job.getInstance();
		conf = job.getConfiguration();
		String linkedWB2FileName = "excel2003linkedwb2b";
		outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", linkedWB2FileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		jContext = new JobContextImpl(conf, taskID.getJobID());

		context = new TaskAttemptContextImpl(conf, taskID);
		committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		outputFormat = new ExcelFileOutputFormat();
		writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, wb2a1);
		writer.write(null, wb2b1);
		writer.write(null, wb2c1);
		writer.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// write mainworkbook
		linkedWB1FileName = linkedWB1FileName + this.outputbaseAppendix;
		linkedWB2FileName = linkedWB2FileName + this.outputbaseAppendix;
		String linkedWorkbookFilename = "[" + tmpDir + File.separator + linkedWB1FileName + ".xls]:[" + tmpDir
				+ File.separator + linkedWB2FileName + ".xls]";
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test7", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("", "", "[" + linkedWB1FileName + ".xls]Sheet1!B1", "B1",
				"Sheet1"); // should be test2 in the end
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("", "", "[" + linkedWB2FileName + ".xls]Sheet1!B1", "C1",
				"Sheet1"); // should be test5 in the end
		// write
		job = Job.getInstance();
		conf = job.getConfiguration();
		String mainWBfileName = "excel2003singlesheetlinkedwb2testout";
		outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", mainWBfileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel");
		conf.set("hadoopoffice.write.linkedworkbooks", linkedWorkbookFilename);
		conf.set("hadoopoffice.write.mimetype", "application/vnd.ms-excel");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		jContext = new JobContextImpl(conf, taskID.getJobID());

		context = new TaskAttemptContextImpl(conf, taskID);
		committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writerMain = outputFormat.getRecordWriter(context);
		assertNotNull(writerMain, "Format returned  null RecordWriter");
		writerMain.write(null, a1);
		writerMain.write(null, b1);
		writerMain.write(null, c1);
		writerMain.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		mainWBfileName = mainWBfileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + mainWBfileName + ".xls");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 Sheet1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + mainWBfileName + ".xls]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + mainWBfileName + ".xls]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length,
				"Input Split for Excel file contains row 1 with 3 columns for Sheet1");
		assertEquals("test7", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test7\"");
		// this comes from the external workbook
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test5", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test5\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013TemplateSingleSheet() throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// change the cell A4 from Test4 to Test5 from the template
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("Test5", "", "", "A4", "Table1");
		// change b4 from 10 to 60
		SpreadSheetCellDAO b4 = new SpreadSheetCellDAO("", "", "60", "B4", "Table1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String fileName = "excel2013basedontemplate";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// template
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameTemplate = classLoader.getResource("templatetest1.xlsx").getFile();
		conf.set("hadoopoffice.write.template.file", fileNameTemplate);
		//
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		// set generic outputformat settings
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a4);
		writer.write(null, b4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Table1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Table1!A1\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 2 columns");
		assertEquals("Test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"Test\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"10\"");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 2 columns");
		assertEquals("Test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"Test2\"");
		assertEquals("50", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 2 == \"50\"");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 2 columns");
		assertEquals("Test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"Test3\"");
		assertEquals("20", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"20\"");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 2 columns");
		assertEquals("Test5", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"Test5\"");

		assertEquals("60", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"60\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013TemplateEncryptedSingleSheetPositive()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// change the cell A4 from Test4 to Test5 from the template
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("Test5", "", "", "A4", "Table1");
		// change b4 from 10 to 60
		SpreadSheetCellDAO b4 = new SpreadSheetCellDAO("", "", "60", "B4", "Table1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013basedontemplateencrypted";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// template
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameTemplate = classLoader.getResource("templatetest1encrypt.xlsx").getFile();
		conf.set("hadoopoffice.write.template.file", fileNameTemplate);
		conf.set("hadoopoffice.write.template.password", "test");
		//
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		// set generic outputformat settings
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a4);
		writer.write(null, b4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Table1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Table1!A1\"");
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 2 columns");
		assertEquals("Test", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"Test\"");
		assertEquals("10", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"10\"");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contains row 2 with 2 columns");
		assertEquals("Test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 1 == \"Test2\"");
		assertEquals("50", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 2 with cell 2 == \"50\"");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 2 columns");
		assertEquals("Test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"Test3\"");
		assertEquals("20", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"20\"");

		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(2, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 2 columns");
		assertEquals("Test5", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"Test5\"");

		assertEquals("60", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"60\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013TemplateEncryptedSingleSheetNegative() throws IOException {
		// one row string and three columns ("test1","test2","test3")
		// change the cell A4 from Test4 to Test5 from the template
		SpreadSheetCellDAO a4 = new SpreadSheetCellDAO("Test5", "", "", "A4", "Table1");
		// change b4 from 10 to 60
		SpreadSheetCellDAO b4 = new SpreadSheetCellDAO("", "", "60", "B4", "Table1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013basedontemplateencrypted";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// template
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameTemplate = classLoader.getResource("templatetest1encrypt.xlsx").getFile();
		conf.set("hadoopoffice.write.template.file", fileNameTemplate);
		conf.set("hadoopoffice.write.template.password", "test2");
		//
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		// set generic outputformat settings
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);

		assertNull(writer, "Format returned  null RecordWriter");
	}

	@Test
	public void readExcelInputFormatExcel2013SingleSheetEncryptedKeyStorePositive()
			throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		String keystoreFilename = "keystore.jceks";
		String filenameKeyStore = classLoader.getResource(keystoreFilename).getFile().toString();
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// for decryption set the keystore to retrieve the password
		conf.set("hadoopoffice.read.security.crypt.credential.keystore.file", filenameKeyStore);
		conf.set("hadoopoffice.read.security.crypt.credential.keystore.type", "JCEKS");
		conf.set("hadoopoffice.read.security.crypt.credential.keystore.password", "changeit");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
	public void readExcelInputFormatExcel2013SingleSheetEncryptedKeyStoreAliasPositive()
			throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		String keystoreFilename = "keystore.jceks";
		String filenameKeyStore = classLoader.getResource(keystoreFilename).getFile().toString();
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// for decryption set the keystore to retrieve the password
		conf.set("hadoopoffice.read.security.crypt.credential.keystore.file", filenameKeyStore);
		conf.set("hadoopoffice.read.security.crypt.credential.keystore.type", "JCEKS");
		conf.set("hadoopoffice.read.security.crypt.credential.keystore.password", "changeit");

		conf.set("hadoopoffice.read.security.crypt.credential.keystore.alias", "testalias");
		Job job = Job.getInstance(conf);
		FileInputFormat.setInputPaths(job, file);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		ExcelFileInputFormat format = new ExcelFileInputFormat();
		List<InputSplit> splits = format.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = format.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
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
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedKeyStorePositive()
			throws IOException, InterruptedException {
		ClassLoader classLoader = getClass().getClassLoader();
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutencryptedkeystorepositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set generic outputformat settings
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		conf.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		conf.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		conf.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		conf.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		// retrieve password for encryption from keystore
		String keystoreFilename = "keystore.jceks";
		String filenameKeyStore = classLoader.getResource(keystoreFilename).getFile().toString();
		conf.set("hadoopoffice.write.security.crypt.credential.keystore.file", filenameKeyStore);
		conf.set("hadoopoffice.write.security.crypt.credential.keystore.type", "JCEKS");
		conf.set("hadoopoffice.write.security.crypt.credential.keystore.password", "changeit");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		conf.set("hadoopoffice.read.security.crypt.password", "test");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedKeyStoreAliasPositive()
			throws IOException, InterruptedException {
		ClassLoader classLoader = getClass().getClassLoader();
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutencryptedkeystorepositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set generic outputformat settings
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		conf.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		conf.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		conf.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		conf.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		// retrieve password for encryption from keystore
		String keystoreFilename = "keystore.jceks";
		String filenameKeyStore = classLoader.getResource(keystoreFilename).getFile().toString();
		conf.set("hadoopoffice.write.security.crypt.credential.keystore.file", filenameKeyStore);
		conf.set("hadoopoffice.write.security.crypt.credential.keystore.type", "JCEKS");
		conf.set("hadoopoffice.write.security.crypt.credential.keystore.password", "changeit");

		conf.set("hadoopoffice.write.security.crypt.credential.keystore.alias", "testalias");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		conf.set("hadoopoffice.read.security.crypt.password", "test");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}



	@Disabled("We need to update the test certificate with certificate revocation lists (CRL)")
	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetSignedCertChainPositive()
			throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutsignedcertchainpositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
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
		String pkFileName = "testsigningCA.pfx"; // private key
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameKeyStore = classLoader.getResource(pkFileName).getFile();

		conf.set("hadoopoffice.write.security.sign.keystore.file", fileNameKeyStore);
		conf.set("hadoopoffice.write.security.sign.keystore.type", "PKCS12");
		conf.set("hadoopoffice.write.security.sign.keystore.password", "changeit");
		conf.set("hadoopoffice.write.security.sign.keystore.alias", "testalias");
		conf.set("hadoopoffice.write.security.sign.hash.algorithm", "sha512");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		// set generic outputformat settings
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.read.security.sign.verifysignature", "true");
		// trust store for certfication chain
		String trustFileName = "signingtruststore.jks";
		String fileNameTrustStore = classLoader.getResource(trustFileName).getFile();
		conf.set("hadoopoffice.read.security.sign.truststore.file", fileNameTrustStore);
		conf.set("hadoopoffice.read.security.sign.truststore.type", "JKS");
		conf.set("hadoopoffice.read.security.sign.truststore.password", "changeit");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetSignedNegative() throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutsignednegative";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes

		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		// set generic outputformat settings
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.read.security.sign.verifysignature", "true"); // will fail because no signature present
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);

		TaskAttemptContext context2 = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context2);
		InterruptedException ex = assertThrows(InterruptedException.class,
				() -> reader.initialize(splits.get(0), context2),
				"Exception is thrown in case signature cannot be verified");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedPositiveSignedPositive()
			throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutencryptedpositivesignedpositive";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set generic outputformat settings
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		conf.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		conf.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		conf.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		conf.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		conf.set("hadoopoffice.write.security.crypt.password", "test");
		/// signature
		String pkFileName = "testsigning.pfx"; // private key
		ClassLoader classLoader = getClass().getClassLoader();
		String fileNameKeyStore = classLoader.getResource(pkFileName).getFile();

		conf.set("hadoopoffice.write.security.sign.keystore.file", fileNameKeyStore);
		conf.set("hadoopoffice.write.security.sign.keystore.type", "PKCS12");
		conf.set("hadoopoffice.write.security.sign.keystore.password", "changeit");
		conf.set("hadoopoffice.write.security.sign.keystore.alias", "testalias");
		conf.set("hadoopoffice.write.security.sign.hash.algorithm", "sha512");
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		conf.set("hadoopoffice.read.security.crypt.password", "test");

		conf.set("hadoopoffice.read.security.sign.verifysignature", "true");
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + fileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + fileName + ".xlsx]Sheet1!A1\"");
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contains row 1 with 3 columns");
		assertEquals("test1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test1\"");
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 2");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(0, spreadSheetValue.get().length, "Input Split for Excel file contain row 2 and is empty");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 3");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(3, spreadSheetValue.get().length, "Input Split for Excel file contain row 3 with 3 columns");
		assertEquals("1", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"1\"");
		assertEquals("2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 2 == \"2\"");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 3 == \"3\"");
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 4");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals(1, spreadSheetValue.get().length, "Input Split for Excel file contain row 4 with 1 column");
		assertEquals("3", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 3 with cell 1 == \"3\"");
	}

	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetEncryptedPositiveSignedNegative()
			throws IOException, InterruptedException {
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
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutencryptedpositivesignednegative";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);
		// set generic outputformat settings
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// excel
																														// format
		// security
		// for the new Excel format you need to decide on your own which algorithms are
		// secure
		conf.set("hadoopoffice.write.security.crypt.encrypt.mode", "agile");
		conf.set("hadoopoffice.write.security.crypt.encrypt.algorithm", "aes256");
		conf.set("hadoopoffice.write.security.crypt.chain.mode", "cbc");
		conf.set("hadoopoffice.write.security.crypt.hash.algorithm", "sha512");
		conf.set("hadoopoffice.write.security.crypt.password", "test");

		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, a1);
		writer.write(null, b1);
		writer.write(null, c1);
		writer.write(null, a3);
		writer.write(null, b3);
		writer.write(null, c3);
		writer.write(null, a4);
		writer.close(context);
		committer.commitTask(context);
		// try to read it again
		conf = new Configuration(defaultConf);
		job = Job.getInstance(conf);
		fileName = fileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + "_temporary" + File.separator + "0" + File.separator
				+ taskAttempt + File.separator + fileName + ".xlsx");
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// you just need to provide the password to read encrypted data
		conf.set("hadoopoffice.read.security.crypt.password", "test");

		conf.set("hadoopoffice.read.security.sign.verifysignature", "true"); // will fail no signature provided
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		FileInputFormat.setInputPaths(job, inputFile);
		TaskAttemptContext context2 = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context2);
		InterruptedException ex = assertThrows(InterruptedException.class,
				() -> reader.initialize(splits.get(0), context2),
				"Exception is thrown in case signature cannot be verified");
	}



	@Disabled("This does not work yet due to a bug in Apache POI that prevents writing correct workbooks containing external references: https://bz.apache.org/bugzilla/show_bug.cgi?id=57184")
	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetOneLinkedWorkbook() throws IOException, InterruptedException {
		// write linkedworkbook1
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO wb1a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO wb1b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO wb1c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String linkedWB1FileName = "excel2013linkedwb1";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", linkedWB1FileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, wb1a1);
		writer.write(null, wb1b1);
		writer.write(null, wb1c1);
		writer.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// write mainworkbook
		linkedWB1FileName = linkedWB1FileName + this.outputbaseAppendix;
		String linkedWorkbookFilename = "[" + tmpDir + File.separator + linkedWB1FileName + ".xlsx]";
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test4", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("", "", "[" + linkedWB1FileName + ".xlsx]Sheet1!B1", "B1",
				"Sheet1"); // should be test2 in the end
		// write
		job = Job.getInstance();
		conf = job.getConfiguration();

		String mainWBfileName = "excel2013singlesheetlinkedwbtestout";
		outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", mainWBfileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set("hadoopoffice.write.linkedworkbooks", linkedWorkbookFilename);
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		jContext = new JobContextImpl(conf, taskID.getJobID());

		context = new TaskAttemptContextImpl(conf, taskID);
		committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writerMain = outputFormat.getRecordWriter(context);
		assertNotNull(writerMain, "Format returned  null RecordWriter");
		writerMain.write(null, a1);
		writerMain.write(null, b1);
		writerMain.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// try to read it again
		job = Job.getInstance(conf);
		mainWBfileName = mainWBfileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + mainWBfileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 Sheet1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + mainWBfileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + mainWBfileName + ".xlsx]Sheet1!A1\"");
		assertEquals(2, spreadSheetValue.get().length,
				"Input Split for Excel file contains row 1 with 2 columns for Sheet1");
		assertEquals("test4", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test4\"");
		// this comes from the external workbook
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
	}

	@Disabled("This does not work yet due to a bug in Apache POI that prevents writing correct workbooks containing external references: https://bz.apache.org/bugzilla/show_bug.cgi?id=57184")
	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetTwoLinkedWorkbooks()
			throws IOException, InterruptedException {
		// write linkedworkbook1
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO wb1a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO wb1b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO wb1c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");
		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		String linkedWB1FileName = "excel2013linkedwb1";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", linkedWB1FileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		JobContext jContext = new JobContextImpl(conf, taskID.getJobID());

		TaskAttemptContext context = new TaskAttemptContextImpl(conf, taskID);
		FileOutputCommitter committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);
		ExcelFileOutputFormat outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, wb1a1);
		writer.write(null, wb1b1);
		writer.write(null, wb1c1);
		writer.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// write linkedworkbook2
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO wb2a1 = new SpreadSheetCellDAO("test4", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO wb2b1 = new SpreadSheetCellDAO("test5", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO wb2c1 = new SpreadSheetCellDAO("test6", "", "", "C1", "Sheet1");
		// write
		job = Job.getInstance();
		conf = job.getConfiguration();
		String linkedWB2FileName = "excel2013linkedwb2";
		outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", linkedWB2FileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		jContext = new JobContextImpl(conf, taskID.getJobID());

		context = new TaskAttemptContextImpl(conf, taskID);
		committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.commitTask(context);
		committer.commitJob(jContext);
		outputFormat = new ExcelFileOutputFormat();
		writer = outputFormat.getRecordWriter(context);
		assertNotNull(writer, "Format returned  null RecordWriter");
		writer.write(null, wb2a1);
		writer.write(null, wb2b1);
		writer.write(null, wb2c1);
		writer.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// write mainworkbook
		linkedWB1FileName = linkedWB1FileName + this.outputbaseAppendix;
		linkedWB2FileName = linkedWB2FileName + this.outputbaseAppendix;
		String linkedWorkbookFilename = "[" + tmpDir + File.separator + linkedWB1FileName + ".xlsx]:[" + tmpDir
				+ File.separator + linkedWB2FileName + ".xlsx]";
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test7", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("", "", "'[" + linkedWB1FileName + ".xlsx]Sheet1'!B1", "B1",
				"Sheet1"); // should be test2 in the end
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("", "", "'[" + linkedWB2FileName + ".xlsx]Sheet1'!B1", "B1",
				"Sheet1"); // should be test5 in the end
		// write
		job = Job.getInstance();
		conf = job.getConfiguration();
		String mainWBfileName = "excel2013singlesheetlinkedwbtestout";
		outputPath = new Path(tmpDir);
		FileOutputFormat.setOutputPath(job, outputPath);
		conf.set("mapreduce.output.basename", mainWBfileName);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		conf.set("hadoopoffice.write.mimetype", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"); // new
																														// Excel
																														// format,
																														// anyway
																														// default,
																														// but
																														// here
																														// for
																														// illustrative
																														// purposes
		conf.set("hadoopoffice.write.linkedworkbooks", linkedWorkbookFilename);
		conf.set(MRJobConfig.TASK_ATTEMPT_ID, attempt);
		conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
		conf.setInt(FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
		FileOutputFormat.setOutputPath(job, outputPath);
		jContext = new JobContextImpl(conf, taskID.getJobID());

		context = new TaskAttemptContextImpl(conf, taskID);
		committer = new FileOutputCommitter(outputPath, context);
		// setup
		committer.setupJob(jContext);
		committer.setupTask(context);

		outputFormat = new ExcelFileOutputFormat();
		RecordWriter<NullWritable, SpreadSheetCellDAO> writerMain = outputFormat.getRecordWriter(context);
		assertNotNull(writerMain, "Format returned  null RecordWriter");
		writerMain.write(null, a1);
		writerMain.write(null, b1);
		writerMain.write(null, c1);
		writerMain.close(context);
		committer.commitTask(context);
		committer.commitJob(jContext);
		// try to read it again
		job = Job.getInstance(conf);
		mainWBfileName = mainWBfileName + this.outputbaseAppendix;
		Path inputFile = new Path(tmpDir + File.separator + mainWBfileName + ".xlsx");
		FileInputFormat.setInputPaths(job, inputFile);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// enable option to read linked workbooks
		conf.setBoolean("hadoopoffice.read.linkedworkbooks", true);
		conf.setBoolean("hadoopoffice.read.ignoremissinglinkedworkbooks", false);
		ExcelFileInputFormat inputFormat = new ExcelFileInputFormat();
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		List<InputSplit> splits = inputFormat.getSplits(job);
		assertEquals(1, splits.size(), "Only one split generated for Excel file");
		RecordReader<Text, ArrayWritable> reader = inputFormat.createRecordReader(splits.get(0), context);
		assertNotNull(reader, "Format returned  null RecordReader");
		reader.initialize(splits.get(0), context);
		Text spreadSheetKey = new Text();
		ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
		assertTrue(reader.nextKeyValue(), "Input Split for Excel file contains row 1 Sheet1");
		spreadSheetKey = reader.getCurrentKey();
		spreadSheetValue = reader.getCurrentValue();
		assertEquals("[" + mainWBfileName + ".xlsx]Sheet1!A1", spreadSheetKey.toString(),
				"Input Split for Excel file has keyname == \"[" + mainWBfileName + ".xlsx]Sheet1!A1\"");
		assertEquals(2, spreadSheetValue.get().length,
				"Input Split for Excel file contains row 1 with 2 columns for Sheet1");
		assertEquals("test7", ((SpreadSheetCellDAO) spreadSheetValue.get()[0]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 1 == \"test7\"");
		// this comes from the external workbook
		assertEquals("test2", ((SpreadSheetCellDAO) spreadSheetValue.get()[1]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 2 == \"test2\"");
		assertEquals("test5", ((SpreadSheetCellDAO) spreadSheetValue.get()[2]).getFormattedValue(),
				"Input Split for Excel file contains row 1 with cell 3 == \"test5\"");
	}

}

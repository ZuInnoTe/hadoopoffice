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
package org.zuinnote.hadoop.office.format.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

public class OfficeFormatHadoopExcelLowFootPrintStaXTest {
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootPrintStax() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		// stax parser
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootPrintStaxAllMemory() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		// stax parser
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
		// all memory
		conf.set("hadoopoffice.read.lowfootprint.stax.sst.cache", "-1");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootPrintStaxNothingInMemory() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		// stax parser
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
		// nothing memory
		conf.set("hadoopoffice.read.lowfootprint.stax.sst.cache", "0");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootPrintStaxPartlyInMemory() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		// stax parser
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
		// partly in memory
		conf.set("hadoopoffice.read.lowfootprint.stax.sst.cache", "2");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootPrintStaxPartlyInMemoryCompressed() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		// stax parser
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
		// partly in memory compressed
		conf.set("hadoopoffice.read.lowfootprint.stax.sst.cache", "1");
		conf.set("hadoopoffice.read.lowfootprint.stax.sst.compress", "true");
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
	public void readExcelInputFormatExcel2013MultiSheetHeaderLowFootPrint() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "multisheetheader.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.locale.bcp47", "us");
		conf.set("hadoopoffice.read.header.read", "true");
		conf.set("hadoopoffice.read.header.skipheaderinallsheets", "true");
		conf.set("hadoopoffice.read.lowfootprint", "true");

		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013MultiSheetHeaderRegExLowFootprint() throws IOException, InterruptedException {
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
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013SingleSheetLowFootPrint() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013MultiSheetAllLowFootPrint() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013SingleSheetEncryptedNegativeLowFootprint()
			throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);
		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013SingleSheetEncryptedPositiveLowFootprint()
			throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");
		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void readExcelInputFormatExcel2013EmptyRowsLowFootprint() throws IOException, InterruptedException {
		Configuration conf = new Configuration(defaultConf);
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testemptyrows.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		Path file = new Path(fileNameSpreadSheet);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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
	public void writeExcelOutputFormatExcel2013SingleSheetLowFootprintSignedPositiveReadLowFootprint()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");

		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutsignedpositivereadlowfootprint";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		conf.set("hadoopoffice.write.lowfootprint", "true");
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
		// set generic outputformat settings
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

		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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

	}
	@Test
	public void writeExcelOutputFormatExcel2013SingleSheetLowFootprintSignedNegativeReadLowFootprint()
			throws IOException, InterruptedException {
		// one row string and three columns ("test1","test2","test3")
		// (String formattedValue, String comment, String formula, String address,String
		// sheetName)
		SpreadSheetCellDAO a1 = new SpreadSheetCellDAO("test1", "", "", "A1", "Sheet1");
		SpreadSheetCellDAO b1 = new SpreadSheetCellDAO("test2", "", "", "B1", "Sheet1");
		SpreadSheetCellDAO c1 = new SpreadSheetCellDAO("test3", "", "", "C1", "Sheet1");

		// write
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();

		String fileName = "excel2013singlesheettestoutsignednegativereadlowfootprint";
		String tmpDir = tmpPath.toString();
		Path outputPath = new Path(tmpDir);
		conf.set("mapreduce.output.basename", fileName);

		// set locale to the one of the test data
		conf.set("hadoopoffice.read.locale.bcp47", "de");

		// low footprint
		conf.set("hadoopoffice.write.lowfootprint", "true");
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

		// low footprint
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
		conf.set("hadoopoffice.read.security.sign.verifysignature", "true"); // will fail because no signature provided
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
	public void readExcelInputFormatExcel2013MultiSheetSkipWithHeaderLowFootprint()
			throws IOException, InterruptedException {
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
		conf.set("hadoopoffice.read.lowfootprint", "true");
		conf.set("hadoopoffice.read.lowfootprint.parser", "stax");
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

}

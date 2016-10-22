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


package org.zuinnote.hadoop.office.format;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;


import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;


import java.io.File;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;

import org.apache.hadoop.util.ReflectionUtils;

import org.zuinnote.hadoop.office.format.dao.*;

public class OfficeFormatHadoopTest {
private static JobConf defaultConf = new JobConf();
private static FileSystem localFs = null; 
private static Reporter reporter = Reporter.NULL;

   @BeforeClass
    public static void oneTimeSetUp() throws IOException {
      // one-time initialization code   
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    }

    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
      }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {  
    }

    @Test
    public void checkTestExcel2003EmptySheetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2003empty.xls";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }

    @Test
    public void checkTestExcel2013EmptySheetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2013empty.xlsx";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }

    @Test
    public void checkTestExcel2003SingleSheetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2003test.xls";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }

    @Test
    public void checkTestExcel2013SingleSheetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2013test.xlsx";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }

    @Test
    public void checkTestExcel2013MultiSheetAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2013testmultisheet.xlsx";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }

    @Test
    public void checkTestExcel2013CommentAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2013comment.xlsx";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }
    @Test
    public void checkTestExcel2013MultiSheetGzipAvailable() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2013testmultisheet.xlsx.gz";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }

    @Test
    public void checkTestExcel2013MultiSheetBzip2Available() {
	ClassLoader classLoader = getClass().getClassLoader();
	String fileName="excel2013testmultisheet.xlsx.bz2";
	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
	assertNotNull("Test Data File \""+fileName+"\" is not null in resource path",fileNameSpreadSheet);
	File file = new File(fileNameSpreadSheet);
	assertTrue("Test Data File \""+fileName+"\" exists", file.exists());
	assertFalse("Test Data File \""+fileName+"\" is not a directory", file.isDirectory());
    }

    @Test
    public void readExcelInputFormatExcel2003Empty() throws IOException {
JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2003empty.xls";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertFalse("Input Split for Excel file contains no row", reader.next(spreadSheetKey,spreadSheetValue));	
    }

   @Test
    public void readExcelInputFormatExcel2013Empty() throws IOException {
        JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013empty.xlsx";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertFalse("Input Split for Excel file contains no row", reader.next(spreadSheetKey,spreadSheetValue));	
    }

    @Test
    public void readExcelInputFormatExcel2003SingleSheet() throws IOException {
    	JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2003test.xls";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 1", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file has keyname == \"Sheet1!A1\"", "Sheet1!A1", spreadSheetKey.toString());
	assertEquals("Input Split for Excel file contains row 1 with 4 columns", 4, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 1 with cell 1 == \"test1\"", "test1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"", "Sheet1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getSheetName());	
	assertEquals("Input Split for Excel file contains row 1 with cell 1 address == \"A1\"", "A1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getAddress());	
assertEquals("Input Split for Excel file contains row 1 with cell 2 == \"test2\"", "test2", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
assertEquals("Input Split for Excel file contains row 1 with cell 3 == \"test3\"", "test3", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
assertEquals("Input Split for Excel file contains row 1 with cell 4 == \"test4\"", "test4", ((SpreadSheetCellDAO)spreadSheetValue.get()[3]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 2", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 2 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 2 with cell 1 == \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 3", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 3 with 5 columns", 5, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"", "31/12/99", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 3 with cell 2 == \"5\"", "5", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertNull("Input Split for Excel file contains row 3 with cell 3 == null", spreadSheetValue.get()[2]);	
	assertNull("Input Split for Excel file contains row 3 with cell 4 == null", spreadSheetValue.get()[3]);	
	assertEquals("Input Split for Excel file contains row 3 with cell 5 == \"null\"", "null", ((SpreadSheetCellDAO)spreadSheetValue.get()[4]).getFormattedValue());		
	assertTrue("Input Split for Excel file contains row 4", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 4 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 4 with cell 1 == \"1\"", "1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 5", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 5 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 5 with cell 1 == \"2\"", "2", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());			 
	assertEquals("Input Split for Excel file contains row 5 with cell 2== \"6\"", "6", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 5 with cell 3== \"10\"", "10", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 6", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 6 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 6 with cell 1 == \"3\"", "3", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());		 
	assertEquals("Input Split for Excel file contains row 6 with cell 2== \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 6 with cell 3== \"15\"", "15", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
    }


    @Test
    public void readExcelInputFormatExcel2013SingleSheet() throws IOException {
	JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013test.xlsx";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 1", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file has keyname == \"Sheet1!A1\"", "Sheet1!A1", spreadSheetKey.toString());
	assertEquals("Input Split for Excel file contains row 1 with 4 columns", 4, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 1 with cell 1 == \"test1\"", "test1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"", "Sheet1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getSheetName());	
	assertEquals("Input Split for Excel file contains row 1 with cell 1 address == \"A1\"", "A1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getAddress());	
	assertEquals("Input Split for Excel file contains row 1 with cell 2 == \"test2\"", "test2", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 3 == \"test3\"", "test3", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 4 == \"test4\"", "test4", ((SpreadSheetCellDAO)spreadSheetValue.get()[3]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 2", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 2 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 2 with cell 1 == \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 3", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 3 with 5 columns", 5, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"", "31/12/99", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 3 with cell 2 == \"5\"", "5", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertNull("Input Split for Excel file contains row 3 with cell 3 == null", spreadSheetValue.get()[2]);	
	assertNull("Input Split for Excel file contains row 3 with cell 4 == null", spreadSheetValue.get()[3]);	
	assertEquals("Input Split for Excel file contains row 3 with cell 5 == \"null\"", "null", ((SpreadSheetCellDAO)spreadSheetValue.get()[4]).getFormattedValue());		
	assertTrue("Input Split for Excel file contains row 4", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 4 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 4 with cell 1 == \"1\"", "1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 5", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 5 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 5 with cell 1 == \"2\"", "2", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());			 
	assertEquals("Input Split for Excel file contains row 5 with cell 2== \"6\"", "6", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 5 with cell 3== \"10\"", "10", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 6", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 6 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 6 with cell 1 == \"3\"", "3", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());		 
	assertEquals("Input Split for Excel file contains row 6 with cell 2== \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 6 with cell 3== \"15\"", "15", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
    }


    @Test
    public void readExcelInputFormatExcel2013MultiSheetAll() throws IOException {
	JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013testmultisheet.xlsx";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 1 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file has keyname == \"Sheet1!A1\"", "Sheet1!A1", spreadSheetKey.toString());
	assertEquals("Input Split for Excel file contains row 1 with 4 columns", 4, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 1 with cell 1 == \"test1\"", "test1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"", "Sheet1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getSheetName());	
	assertEquals("Input Split for Excel file contains row 1 with cell 1 address == \"A1\"", "A1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getAddress());	
	assertEquals("Input Split for Excel file contains row 1 with cell 2 == \"test2\"", "test2", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 3 == \"test3\"", "test3", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 4 == \"test4\"", "test4", ((SpreadSheetCellDAO)spreadSheetValue.get()[3]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 2 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 2 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 2 with cell 1 == \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 3 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 3 with 5 columns", 5, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"", "31/12/99", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 3 with cell 2 == \"5\"", "5", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertNull("Input Split for Excel file contains row 3 with cell 3 == null", spreadSheetValue.get()[2]);	
	assertNull("Input Split for Excel file contains row 3 with cell 4 == null", spreadSheetValue.get()[3]);	
	assertEquals("Input Split for Excel file contains row 3 with cell 5 == \"null\"", "null", ((SpreadSheetCellDAO)spreadSheetValue.get()[4]).getFormattedValue());		
	assertTrue("Input Split for Excel file contains row 4 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 4 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 4 with cell 1 == \"1\"", "1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 5 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 5 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 5 with cell 1 == \"2\"", "2", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());			 
	assertEquals("Input Split for Excel file contains row 5 with cell 2== \"6\"", "6", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 5 with cell 3== \"10\"", "10", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 6 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 6 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 6 with cell 1 == \"3\"", "3", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());		 
	assertEquals("Input Split for Excel file contains row 6 with cell 2== \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 6 with cell 3== \"15\"", "15", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 7 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 7 with cell 1 == \"8\"", "8", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with cell 2 == \"99\"", "99", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with 2 columns", 2, spreadSheetValue.get().length);
	assertTrue("Input Split for Excel file contains row 8 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 8 with 1 column",1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 8 with cell 1 == \"test\"", "test", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 9 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 9 with 3 columns", 3, spreadSheetValue.get().length);
	assertNull("Input Split for Excel file contains row 9 with cell 1 == null", spreadSheetValue.get()[0]);	
	assertNull("Input Split for Excel file contains row 9 with cell 2 == null", spreadSheetValue.get()[1]);	
	assertEquals("Input Split for Excel file contains row 9 with cell 3 == \"seven\"", "seven", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
    }

@Test
    public void readExcelInputFormatExcel2013MultiSheetSelectedSheet() throws IOException {
	JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013testmultisheet.xlsx";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
	// select the sheet	
	job.set("hadoopoffice.sheets","testsheet");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 7 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 7 with cell 1 == \"8\"", "8", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with cell 2 == \"99\"", "99", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with 2 columns", 2, spreadSheetValue.get().length);
	assertTrue("Input Split for Excel file contains row 8 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 8 with 1 column",1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 8 with cell 1 == \"test\"", "test", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 9 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 9 with 3 columns", 3, spreadSheetValue.get().length);
	assertNull("Input Split for Excel file contains row 9 with cell 1 == null", spreadSheetValue.get()[0]);	
	assertNull("Input Split for Excel file contains row 9 with cell 2 == null", spreadSheetValue.get()[1]);	
	assertEquals("Input Split for Excel file contains row 9 with cell 3 == \"seven\"", "seven", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
    }


    @Test
    public void readExcelInputFormatExcel2013Comment() throws IOException {
JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013comment.xlsx";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 1", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file has keyname == \"CommentSheet!A1\"", "CommentSheet!A1", spreadSheetKey.toString());
	assertEquals("Input Split for Excel file contains row 1 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 1 with cell 1 == \"test1\"", "test1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 1 with cell 1 comment == \"First comment\"", "First comment", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getComment());
	assertEquals("Input Split for Excel file contains row 1 with cell 1 sheetname == \"CommentSheet\"", "CommentSheet", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getSheetName());	
	assertEquals("Input Split for Excel file contains row 1 with cell 1 address == \"A1\"", "A1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getAddress());	
	assertEquals("Input Split for Excel file contains row 1 with cell 2 == \"test3\"", "test3", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 3 == \"test3\"", "test3", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 2", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 2 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 2 with cell 1 == \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 3", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 3 with 2 columns", 2, spreadSheetValue.get().length);
	assertNull("Input Split for Excel file contains row 3 with cell 1 == null", spreadSheetValue.get()[0]);	
	assertEquals("Input Split for Excel file contains row 2 with cell 2 == \"5\"", "5", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 2 with cell 2 comment == \"Second comment\"", "Second comment", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getComment());	
	assertTrue("Input Split for Excel file contains row 4", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 4 with 3 column", 3, spreadSheetValue.get().length);
	assertNull("Input Split for Excel file contains row 4 with cell 1 == null", spreadSheetValue.get()[0]);	
	assertNull("Input Split for Excel file contains row 4 with cell 2 == null", spreadSheetValue.get()[1]);	
	assertEquals("Input Split for Excel file contains row 4 with cell 3 == \"6\"", "6", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 4 with cell 3 comment == \"Third comment\"", "Third comment", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getComment());		
    }

    @Test
    public void readExcelInputFormatGzipCompressedExcel2013MultiSheetAll() throws IOException {
      JobConf job = new JobConf(defaultConf);
      CompressionCodec gzip = new GzipCodec();
      ReflectionUtils.setConf(gzip, job);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013testmultisheet.xlsx.gz";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 1 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file has keyname == \"Sheet1!A1\"", "Sheet1!A1", spreadSheetKey.toString());
	assertEquals("Input Split for Excel file contains row 1 with 4 columns", 4, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 1 with cell 1 == \"test1\"", "test1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"", "Sheet1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getSheetName());	
	assertEquals("Input Split for Excel file contains row 1 with cell 1 address == \"A1\"", "A1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getAddress());	
	assertEquals("Input Split for Excel file contains row 1 with cell 2 == \"test2\"", "test2", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 3 == \"test3\"", "test3", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 4 == \"test4\"", "test4", ((SpreadSheetCellDAO)spreadSheetValue.get()[3]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 2 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 2 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 2 with cell 1 == \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 3 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 3 with 5 columns", 5, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"", "31/12/99", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 3 with cell 2 == \"5\"", "5", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertNull("Input Split for Excel file contains row 3 with cell 3 == null", spreadSheetValue.get()[2]);	
	assertNull("Input Split for Excel file contains row 3 with cell 4 == null", spreadSheetValue.get()[3]);	
	assertEquals("Input Split for Excel file contains row 3 with cell 5 == \"null\"", "null", ((SpreadSheetCellDAO)spreadSheetValue.get()[4]).getFormattedValue());		
	assertTrue("Input Split for Excel file contains row 4 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 4 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 4 with cell 1 == \"1\"", "1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 5 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 5 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 5 with cell 1 == \"2\"", "2", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());			 
	assertEquals("Input Split for Excel file contains row 5 with cell 2== \"6\"", "6", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 5 with cell 3== \"10\"", "10", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 6 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 6 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 6 with cell 1 == \"3\"", "3", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());		 
	assertEquals("Input Split for Excel file contains row 6 with cell 2== \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 6 with cell 3== \"15\"", "15", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 7 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 7 with cell 1 == \"8\"", "8", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with cell 2 == \"99\"", "99", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with 2 columns", 2, spreadSheetValue.get().length);
	assertTrue("Input Split for Excel file contains row 8 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 8 with 1 column",1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 8 with cell 1 == \"test\"", "test", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 9 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 9 with 3 columns", 3, spreadSheetValue.get().length);
	assertNull("Input Split for Excel file contains row 9 with cell 1 == null", spreadSheetValue.get()[0]);	
	assertNull("Input Split for Excel file contains row 9 with cell 2 == null", spreadSheetValue.get()[1]);	
	assertEquals("Input Split for Excel file contains row 9 with cell 3 == \"seven\"", "seven", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
    }

    @Test
    public void readExcelInputFormatBzip2CompressedExcel2013MultiSheetAll() throws IOException {
       JobConf job = new JobConf(defaultConf);
       CompressionCodec bzip2 = new BZip2Codec();
       ReflectionUtils.setConf(bzip2, job);
	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013testmultisheet.xlsx.bz2";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
	// set locale to the one of the test data
	job.set("hadoopoffice.locale.bcp47","de");
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 1 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file has keyname == \"Sheet1!A1\"", "Sheet1!A1", spreadSheetKey.toString());
	assertEquals("Input Split for Excel file contains row 1 with 4 columns", 4, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 1 with cell 1 == \"test1\"", "test1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 1 with cell 1 sheetname == \"Sheet1\"", "Sheet1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getSheetName());	
	assertEquals("Input Split for Excel file contains row 1 with cell 1 address == \"A1\"", "A1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getAddress());	
	assertEquals("Input Split for Excel file contains row 1 with cell 2 == \"test2\"", "test2", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 3 == \"test3\"", "test3", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 1 with cell 4 == \"test4\"", "test4", ((SpreadSheetCellDAO)spreadSheetValue.get()[3]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 2 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 2 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 2 with cell 1 == \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 3 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 3 with 5 columns", 5, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 3 with cell 1 == \"31/12/99\"", "31/12/99", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 3 with cell 2 == \"5\"", "5", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertNull("Input Split for Excel file contains row 3 with cell 3 == null", spreadSheetValue.get()[2]);	
	assertNull("Input Split for Excel file contains row 3 with cell 4 == null", spreadSheetValue.get()[3]);	
	assertEquals("Input Split for Excel file contains row 3 with cell 5 == \"null\"", "null", ((SpreadSheetCellDAO)spreadSheetValue.get()[4]).getFormattedValue());		
	assertTrue("Input Split for Excel file contains row 4 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 4 with 1 column", 1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 4 with cell 1 == \"1\"", "1", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 5 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 5 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 5 with cell 1 == \"2\"", "2", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());			 
	assertEquals("Input Split for Excel file contains row 5 with cell 2== \"6\"", "6", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 5 with cell 3== \"10\"", "10", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 6 (first sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 6 with 3 columns", 3, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 6 with cell 1 == \"3\"", "3", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());		 
	assertEquals("Input Split for Excel file contains row 6 with cell 2== \"4\"", "4", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());
	assertEquals("Input Split for Excel file contains row 6 with cell 3== \"15\"", "15", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
	assertTrue("Input Split for Excel file contains row 7 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 7 with cell 1 == \"8\"", "8", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with cell 2 == \"99\"", "99", ((SpreadSheetCellDAO)spreadSheetValue.get()[1]).getFormattedValue());	
	assertEquals("Input Split for Excel file contains row 7 with 2 columns", 2, spreadSheetValue.get().length);
	assertTrue("Input Split for Excel file contains row 8 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 8 with 1 column",1, spreadSheetValue.get().length);
	assertEquals("Input Split for Excel file contains row 8 with cell 1 == \"test\"", "test", ((SpreadSheetCellDAO)spreadSheetValue.get()[0]).getFormattedValue());	
	assertTrue("Input Split for Excel file contains row 9 (second sheet)", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 9 with 3 columns", 3, spreadSheetValue.get().length);
	assertNull("Input Split for Excel file contains row 9 with cell 1 == null", spreadSheetValue.get()[0]);	
	assertNull("Input Split for Excel file contains row 9 with cell 2 == null", spreadSheetValue.get()[1]);	
	assertEquals("Input Split for Excel file contains row 9 with cell 3 == \"seven\"", "seven", ((SpreadSheetCellDAO)spreadSheetValue.get()[2]).getFormattedValue());
    }
    
}

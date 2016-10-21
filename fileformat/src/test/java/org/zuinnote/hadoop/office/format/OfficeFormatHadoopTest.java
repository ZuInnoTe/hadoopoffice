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
    public void readExcelInputFormatExcel2003SingleSheet() throws IOException {
    	JobConf job = new JobConf(defaultConf);
    	ClassLoader classLoader = getClass().getClassLoader();
    	String fileName="excel2013test.xlsx";
    	String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
    	Path file = new Path(fileNameSpreadSheet);
    	FileInputFormat.setInputPaths(job, file);
   	ExcelFileInputFormat format = new ExcelFileInputFormat();
    	format.configure(job);
    	InputSplit[] inputSplits = format.getSplits(job,1);
    	assertEquals("Only one split generated for Excel file", 1, inputSplits.length);
    	RecordReader<Text, ArrayWritable> reader = format.getRecordReader(inputSplits[0], job, reporter);
	assertNotNull("Format returned  null RecordReader", reader);
	Text spreadSheetKey = new Text();	
	ArrayWritable spreadSheetValue = new ArrayWritable(SpreadSheetCellDAO.class);
	assertTrue("Input Split for Excel file contains row 1", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 1 with 4 columns", 4, spreadSheetValue.get().length);	
	assertTrue("Input Split for Excel file contains row 2", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 2 with 1 column", 1, spreadSheetValue.get().length);	
	assertTrue("Input Split for Excel file contains row 3", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 2 with 5 columns", 5, spreadSheetValue.get().length);
	assertTrue("Input Split for Excel file contains row 4", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 4 with 1 column", 1, spreadSheetValue.get().length);
	assertTrue("Input Split for Excel file contains row 5", reader.next(spreadSheetKey,spreadSheetValue));
	assertEquals("Input Split for Excel file contains row 5 with 3 columns", 3, spreadSheetValue.get().length);	 
	assertTrue("Input Split for Excel file contains row 6", reader.next(spreadSheetKey,spreadSheetValue));	
	assertEquals("Input Split for Excel file contains row 6 with 3 columns", 3, spreadSheetValue.get().length);
    }


    @Test
    public void readExcelInputFormatExcel2013SingleSheet() {
    }


    @Test
    public void readExcelInputFormatExcel2013MultiSheet() {
    }

    @Test
    public void readExcelInputFormatGzipCompressedExcel2013MultiSheet() {
      JobConf job = new JobConf(defaultConf);
      CompressionCodec gzip = new GzipCodec();
      ReflectionUtils.setConf(gzip, job);
    }

    @Test
    public void readExcelInputFormatBzip2CompressedExcel2013MultiSheet() {
       JobConf job = new JobConf(defaultConf);
       CompressionCodec bzip2 = new BZip2Codec();
       ReflectionUtils.setConf(bzip2, job);
    }
    
}

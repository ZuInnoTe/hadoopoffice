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

package org.zuinnote.hadoop.office.example;


import org.zuinnote.hadoop.office.example.driver.CSV2ExcelDriver;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.MSExcelParser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;

import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

public final class MapReduceExcelOutputIntegrationTest {
	private static final String tmpPrefix = "hcl-integrationtest";
	private static java.nio.file.Path tmpPath;
	private static String CLUSTERNAME="hcl-minicluster";
	private static String DFS_INPUT_DIR_NAME = "/input";
	private static String DFS_OUTPUT_DIR_NAME = "/output";
	private static String DEFAULT_OUTPUT_FILENAME = "part-r-00000";
	private static String DEFAULT_OUTPUT_EXCEL_FILENAME = "part-r-00000.xlsx";
	private static Path DFS_INPUT_DIR = new Path(DFS_INPUT_DIR_NAME);
	private static Path DFS_OUTPUT_DIR = new Path(DFS_OUTPUT_DIR_NAME);
	private static int NOOFNODEMANAGERS=1;
	private static int NOOFDATANODES=4;
	private static boolean STARTTIMELINESERVER=true;
	private static MiniDFSCluster dfsCluster;
	private static MiniMRYarnCluster miniCluster;

	private ArrayList<Decompressor> openDecompressors = new ArrayList<>();

	   @BeforeAll
	    public static void oneTimeSetUp() throws IOException {
	     	// Create temporary directory for HDFS base and shutdownhook 
		// create temp directory
	      tmpPath = Files.createTempDirectory(tmpPrefix);
	      // create shutdown hook to remove temp files (=HDFS MiniCluster) after shutdown, may need to rethink to avoid many threads are created
		Runtime.getRuntime().addShutdownHook(new Thread(
	    	new Runnable() {
	      	@Override
	      	public void run() {
	        	try {
	          		Files.walkFileTree(tmpPath, new SimpleFileVisitor<java.nio.file.Path>() {
		
	            		@Override
	            		public FileVisitResult visitFile(java.nio.file.Path file,BasicFileAttributes attrs)
	                		throws IOException {
	              			Files.delete(file);
	             			return FileVisitResult.CONTINUE;
	        			}

	        		@Override
	        		public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException e) throws IOException {
	          			if (e == null) {
	            				Files.delete(dir);
	            				return FileVisitResult.CONTINUE;
	          			}
	          			throw e;
	        	}
	        	});
	      	} catch (IOException e) {
	        throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e);
	      }
	    }}));
		// Create Configuration
		Configuration conf = new Configuration();
		// create HDFS cluster
		File baseDir = new File(tmpPath.toString()).getAbsoluteFile();
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
	 	 dfsCluster = builder.numDataNodes(NOOFDATANODES).build();	
		// create Yarn cluster
		YarnConfiguration clusterConf = new YarnConfiguration(conf);
		conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString()); 
		conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
		conf.setClass(YarnConfiguration.RM_SCHEDULER,FifoScheduler.class, ResourceScheduler.class);
		miniCluster = new MiniMRYarnCluster(CLUSTERNAME, NOOFNODEMANAGERS, STARTTIMELINESERVER);
		miniCluster.init(conf);
		miniCluster.start();
	    }

	    @AfterAll
	    public static void oneTimeTearDown() {
	   	// destroy Yarn cluster
		miniCluster.stop();
		// destroy HDFS cluster
		dfsCluster.shutdown();
	      }

	    @BeforeEach
	    public void setUp() throws IOException {
		// create input directory
		dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR);
	    }

	    @AfterEach
	    public void tearDown() throws IOException {
		// Remove input and output directory
		dfsCluster.getFileSystem().delete(DFS_INPUT_DIR,true);
		dfsCluster.getFileSystem().delete(DFS_OUTPUT_DIR,true);
		// close any open decompressor
		for (Decompressor currentDecompressor: this.openDecompressors) {
			if (currentDecompressor!=null) {
				 CodecPool.returnDecompressor(currentDecompressor);
			}
	 	}

	    }

	    @Test
	    public void checkTestDataCSVAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="simplecsv.csv";
		String fileNameCSV=classLoader.getResource(fileName).getFile();	
		assertNotNull(fileNameCSV, "Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameCSV);
		assertTrue(file.exists(), "Test Data File \""+fileName+"\" exists");
		assertFalse(file.isDirectory(), "Test Data File \""+fileName+"\" is not a directory");
	     }

	    @Test
	    public void mapReduceCSV2Excel() throws IOException, Exception {
	    	ClassLoader classLoader = getClass().getClassLoader();
	    	// put testdata on DFS
	    	String fileName="simplecsv.csv";
	    	String fileNameFullLocal=classLoader.getResource(fileName).getFile();
	    	Path inputFile=new Path(fileNameFullLocal);
	    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR);
	    	// submit the application
	    	miniCluster.getConfig().set("hadoopoffice.read.locale.bcp47","en");
	         // Let ToolRunner handle generic command-line options
	  	int res = ToolRunner.run(miniCluster.getConfig(), new CSV2ExcelDriver(), new String[]{dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME,dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME}); 
	    	// fetch results
		List<SpreadSheetCellDAO[]> resultLines = readDefaultExcelResults(2);
	    	// compare results
		assertEquals(2,resultLines.size(), "Number of result line is 2");
		assertEquals("1",resultLines.get(0)[0].getFormattedValue(), "Cell A1 has value 1");
		assertEquals("2",resultLines.get(0)[1].getFormattedValue(), "Cell B1 has value 2");
		assertEquals("3",resultLines.get(0)[2].getFormattedValue(), "Cell C1 has value 3");
		assertEquals("4",resultLines.get(0)[3].getFormattedValue(), "Cell D1 has value 4");
		assertEquals("test1",resultLines.get(1)[0].getFormattedValue(),"Cell A2 has value test1");
		assertEquals("test2",resultLines.get(1)[1].getFormattedValue(), "Cell B2 has value test2");
		assertEquals("test3",resultLines.get(1)[2].getFormattedValue(), "Cell C2 has value test3");
		assertEquals("test4",resultLines.get(1)[3].getFormattedValue(), "Cell D2 has value test4");
	    }
	    
	    
	    /**
	     * Read excel files from the default output directory and default excel outputfile 
	     * @throws FormatNotUnderstoodException 
	     * 
	     * 
	     */
	    
	    private List<SpreadSheetCellDAO[]> readDefaultExcelResults(int numOfRows) throws IOException, FormatNotUnderstoodException {
	    ArrayList<SpreadSheetCellDAO[]> result = new ArrayList<>();
		Path defaultOutputfile = new Path(DFS_OUTPUT_DIR_NAME+"/"+DEFAULT_OUTPUT_EXCEL_FILENAME);
		InputStream defaultInputStream = openFile(defaultOutputfile);	
		// Create a new MS Excel Parser
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration();
		MSExcelParser excelParser = new MSExcelParser(hocr,null);
		excelParser.parse(defaultInputStream);
		for (int i=0;i<numOfRows;i++) {
			SpreadSheetCellDAO[] currentRow = (SpreadSheetCellDAO[]) excelParser.getNext();
			if (currentRow!=null) {
				result.add(currentRow);
			}
		}
		excelParser.close();
	    return result;
	    }
	    

	      /**
		* Read results from the default output directory and default outputfile name
		*
		* @param numOfRows number of rows to read
		*
		*/
	     private List<String> readDefaultResults(int numOfRows) throws IOException {
		ArrayList<String> result = new ArrayList<>();
		Path defaultOutputfile = new Path(DFS_OUTPUT_DIR_NAME+"/"+DEFAULT_OUTPUT_FILENAME);
		InputStream defaultInputStream = openFile(defaultOutputfile);	
		BufferedReader reader=new BufferedReader(new InputStreamReader(defaultInputStream));
		int i=0;
		while(reader.ready())
		{	
			if (i==numOfRows) {
				break;
			}
	     		result.add(reader.readLine());
			i++;
		}
		reader.close();
		return result;
	     }

	/*
	* Opens a file using the Hadoop API. It supports uncompressed and compressed files.
	*
	* @param path path to the file, e.g. file://path/to/file for a local file or hdfs://path/to/file for HDFS file. All filesystem configured for Hadoop can be used
	*
	* @return InputStream from which the file content can be read
	* 
	* @throws java.io.Exception in case there is an issue reading the file
	*
	*
	*/

	private InputStream openFile(Path path) throws IOException {
	        CompressionCodec codec=new CompressionCodecFactory(miniCluster.getConfig()).getCodec(path);
	 	FSDataInputStream fileIn=dfsCluster.getFileSystem().open(path);
		// check if compressed
		if (codec==null) { // uncompressed
			return fileIn;
		} else { // compressed
			Decompressor decompressor = CodecPool.getDecompressor(codec);
			this.openDecompressors.add(decompressor); // to be returned later using close
			if (codec instanceof SplittableCompressionCodec) {
				long end = dfsCluster.getFileSystem().getFileStatus(path).getLen(); 
	        		final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, 0, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
						return cIn;
	      		} else {
	        		return codec.createInputStream(fileIn,decompressor);
	      		}
		}
	}
	
}

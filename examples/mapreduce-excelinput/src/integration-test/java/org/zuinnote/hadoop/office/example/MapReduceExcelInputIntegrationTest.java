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

import org.zuinnote.hadoop.office.example.driver.Excel2CSVDriver;


import java.lang.InterruptedException;

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

public final class MapReduceExcelInputIntegrationTest {
	private static final String tmpPrefix = "hcl-integrationtest";
	private static java.nio.file.Path tmpPath;
	private static String CLUSTERNAME="hcl-minicluster";
	private static String DFS_INPUT_DIR_NAME = "/input";
	private static String DFS_OUTPUT_DIR_NAME = "/output";
	private static String DEFAULT_OUTPUT_FILENAME = "part-r-00000";
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
	    public void checkTestDataExcel2013Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="excel2013test.xlsx";
		String fileNameExcel=classLoader.getResource(fileName).getFile();	
		assertNotNull(fileNameExcel, "Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameExcel);
		assertTrue( file.exists(), "Test Data File \""+fileName+"\" exists");
		assertFalse(file.isDirectory(), "Test Data File \""+fileName+"\" is not a directory");
	     }

	    @Test
	    public void mapReduceExcel2CSV() throws IOException, Exception {
	    	ClassLoader classLoader = getClass().getClassLoader();
	    	// put testdata on DFS
	    	String fileName="excel2013test.xlsx";
	    	String fileNameFullLocal=classLoader.getResource(fileName).getFile();
	    	Path inputFile=new Path(fileNameFullLocal);
	    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR);
	    	// submit the application
	    	miniCluster.getConfig().set("hadoopoffice.read.locale.bcp47","en");
	         // Let ToolRunner handle generic command-line options
	  	int res = ToolRunner.run(miniCluster.getConfig(), new Excel2CSVDriver(), new String[]{dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME,dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME}); 


		 	// fetch results

		
		List<String> resultLines = readDefaultResults(6);
	    	// compare results
		assertEquals(6,resultLines.size(), "Number of result line is 6");
		assertEquals("test1,test2,test3,test4",resultLines.get(0), "First Line CSV correct");
		assertEquals("4",resultLines.get(1), "Second Line CSV correct");
		assertEquals("31/12/99,5,,,null",resultLines.get(2), "Third Line CSV correct");
		assertEquals("1",resultLines.get(3), "Forth Line CSV correct");
		assertEquals("2,6,10",resultLines.get(4), "Fifth Line CSV correct");
		assertEquals("3,4,15",resultLines.get(5), "Sixth Line CSV correct");
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

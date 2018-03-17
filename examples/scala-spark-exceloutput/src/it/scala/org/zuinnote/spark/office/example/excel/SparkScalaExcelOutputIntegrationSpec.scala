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

/**
*
* This test intregrates HDFS and Spark
*
*/

package org.zuinnote.spark.office.example.excel


import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path

import java.io.BufferedReader
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.SimpleFileVisitor
import java.util.ArrayList
import java.util.List


import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import org.apache.hadoop.io.compress.SplitCompressionInputStream

import org.zuinnote.hadoop.office.format.common.util._
import org.zuinnote.hadoop.office.format.common.converter._
import org.zuinnote.hadoop.office.format.common.dao._
import org.zuinnote.hadoop.office.format.common.parser._
import org.zuinnote.hadoop.office.format.common._

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import org.scalatest.{FlatSpec, BeforeAndAfterAll, GivenWhenThen, Matchers}

class SparkScalaExcelOutputIntegrationSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {
 
private var sc: SparkContext = _
private val master: String = "local[2]"
private val appName: String = "example-scalasparkexcelinput-integrationtest"
private val tmpPrefix: String = "ho-integrationtest"
private var tmpPath: java.nio.file.Path = _
private val CLUSTERNAME: String ="hcl-minicluster"
private val DFS_INPUT_DIR_NAME: String = "/input"
private val DFS_OUTPUT_DIR_NAME: String = "/output"
private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
private val DEFAULT_OUTPUT_EXCEL_FILENAME: String = "part-r-00000.xlsx";
private val DFS_INPUT_DIR : Path = new Path(DFS_INPUT_DIR_NAME)
private val DFS_OUTPUT_DIR : Path = new Path(DFS_OUTPUT_DIR_NAME)
private val NOOFDATANODES: Int =4
private var dfsCluster: MiniDFSCluster = _
private var conf: Configuration = _
private var openDecompressors = ArrayBuffer[Decompressor]();

override def beforeAll(): Unit = {
    super.beforeAll()

		// Create temporary directory for HDFS base and shutdownhook 
	// create temp directory
      tmpPath = Files.createTempDirectory(tmpPrefix)
      // create shutdown hook to remove temp files (=HDFS MiniCluster) after shutdown, may need to rethink to avoid many threads are created
	Runtime.getRuntime.addShutdownHook(new Thread("remove temporary directory") {
      	 override def run(): Unit =  {
        	try {
          		Files.walkFileTree(tmpPath, new SimpleFileVisitor[java.nio.file.Path]() {

            		override def visitFile(file: java.nio.file.Path,attrs: BasicFileAttributes): FileVisitResult = {
                		Files.delete(file)
             			return FileVisitResult.CONTINUE
        			}

        		override def postVisitDirectory(dir: java.nio.file.Path, e: IOException): FileVisitResult = {
          			if (e == null) {
            				Files.delete(dir)
            				return FileVisitResult.CONTINUE
          			}
          			throw e
        			}
        	})
      	} catch {
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e)
    }}})
	// create DFS mini cluster
	 conf = new Configuration()
	val baseDir = new File(tmpPath.toString()).getAbsoluteFile()
	conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
	val builder = new MiniDFSCluster.Builder(conf)
 	 dfsCluster = builder.numDataNodes(NOOFDATANODES).build()
	conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString()) 
	// create local Spark cluster
 	val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
	sc = new SparkContext(sparkConf)
 }

  
  override def afterAll(): Unit = {
   // close Spark Context
    if (sc!=null) {
	sc.stop()
    } 
    // close decompressor
	for ( currentDecompressor <- this.openDecompressors) {
		if (currentDecompressor!=null) {
			 CodecPool.returnDecompressor(currentDecompressor)
		}
 	}
    // close dfs cluster
    dfsCluster.shutdown()
    super.afterAll()
}


"The test csv file" should "be converted into a 2 lines Excel" in {
	Given("CSV test file on DFS")
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy CSV input file 
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="simplecsv.csv"
    	val fileNameFullLocal=classLoader.getResource(fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)	
	Given("Configuration")
	conf.set("hadoopoffice.write.locale.bcp47","us");
	When("convert to Excel")
	SparkScalaExcelOut.convertToExcel(sc,conf,dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME,dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
	Then("Excel correspond to CSV")
	val listExcel = readDefaultExcelResults(2)
	assert(2==listExcel.size)
	val firstRow = listExcel.get(0)
	assert(4==firstRow.length) 
	assert("1"==firstRow(0).getFormattedValue())
	assert(""==firstRow(0).getComment())
	assert(""==firstRow(0).getFormula())
	assert("A1"==firstRow(0).getAddress())
	assert("Sheet1"==firstRow(0).getSheetName())
	assert("2"==firstRow(1).getFormattedValue())
	assert(""==firstRow(1).getComment())
	assert(""==firstRow(1).getFormula())
	assert("B1"==firstRow(1).getAddress())
	assert("Sheet1"==firstRow(1).getSheetName())
	assert("3"==firstRow(2).getFormattedValue())
	assert(""==firstRow(2).getComment())
	assert(""==firstRow(2).getFormula())
	assert("C1"==firstRow(2).getAddress())
	assert("Sheet1"==firstRow(2).getSheetName())
	assert("4"==firstRow(3).getFormattedValue())
	assert(""==firstRow(3).getComment())
	assert(""==firstRow(3).getFormula())
	assert("D1"==firstRow(3).getAddress())
	assert("Sheet1"==firstRow(3).getSheetName())
	val secondRow = listExcel.get(1)
	assert(4==secondRow.length) 
	assert("test1"==secondRow(0).getFormattedValue())
	assert(""==secondRow(0).getComment())
	assert(""==secondRow(0).getFormula())
	assert("A2"==secondRow(0).getAddress())
	assert("Sheet1"==secondRow(0).getSheetName())
	assert("test2"==secondRow(1).getFormattedValue())
	assert(""==secondRow(1).getComment())
	assert(""==secondRow(1).getFormula())
	assert("B2"==secondRow(1).getAddress())
	assert("Sheet1"==secondRow(1).getSheetName())
	assert("test3"==secondRow(2).getFormattedValue())
	assert(""==secondRow(2).getComment())
	assert(""==secondRow(2).getFormula())
	assert("C2"==secondRow(2).getAddress())
	assert("Sheet1"==secondRow(2).getSheetName())
	assert("test4"==secondRow(3).getFormattedValue())
	assert(""==secondRow(3).getComment())
	assert(""==secondRow(3).getFormula())
	assert("D2"==secondRow(3).getAddress())
	assert("Sheet1"==secondRow(3).getSheetName())
}



	  
		    
	    /**
	     * Read excel files from the default output directory and default excel outputfile 
	     * @throws FormatNotUnderstoodException 
	     * 
	     * 
	     */
	    
	    def readDefaultExcelResults(numOfRows: Int): List[Array[SpreadSheetCellDAO]] = {
	    val result = new ArrayList[Array[SpreadSheetCellDAO]]()
		val defaultOutputfile = new Path(DFS_OUTPUT_DIR_NAME+"/"+DEFAULT_OUTPUT_EXCEL_FILENAME)
		val defaultInputStream = openFile(defaultOutputfile);
		// Create a new MS Excel Parser
		val hocr = new HadoopOfficeReadConfiguration()
		val excelParser = new MSExcelParser(hocr,null)
		excelParser.parse(defaultInputStream)
		for (i <- 0 to numOfRows-1){
		   val currentRow = excelParser.getNext().asInstanceOf[Array[SpreadSheetCellDAO]]
			if (currentRow!=null) {
				result.add(currentRow)
			}
		}
		excelParser.close()
	    return result
	    }
	        


      /**
	* Read results from the default output directory and default outputfile name
	*
	* @param numOfRows number of rows to read
	*
	*/
     def readDefaultResults(numOfRows: Int): List[String] = {
	val result: ArrayList[String] = new ArrayList[String]()
	val defaultOutputfile = new Path(DFS_OUTPUT_DIR_NAME+"/"+DEFAULT_OUTPUT_FILENAME)
	val defaultInputStream = openFile(defaultOutputfile)
	val reader=new BufferedReader(new InputStreamReader(defaultInputStream))
	var i=0
	while((reader.ready()) && (i!=numOfRows))
	{	
     		result.add(reader.readLine())
		i += 1
	}
	reader.close()
	return result
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

def  openFile(path: Path): InputStream = {
        val codec=new CompressionCodecFactory(conf).getCodec(path)
 	val fileIn: InputStream=dfsCluster.getFileSystem().open(path)
	// check if compressed
	if (codec==null) { // uncompressed
		return fileIn
	} else { // compressed
		val decompressor: Decompressor = CodecPool.getDecompressor(codec)
		openDecompressors+=decompressor // to be returned later using close
		if (codec.isInstanceOf[SplittableCompressionCodec]) {
			val end : Long = dfsCluster.getFileSystem().getFileStatus(path).getLen() 
        		val  cIn =codec.asInstanceOf[SplittableCompressionCodec].createInputStream(fileIn, decompressor, 0, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS)
					return cIn
      		} else {
        		return codec.createInputStream(fileIn,decompressor)
      		}
	}
}

}
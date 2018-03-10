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
* This test intregrates HDFS and Flink
*
*/

package org.zuinnote.flink.office.example.excel


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
import java.util.Locale

import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import org.apache.hadoop.io.compress.SplitCompressionInputStream


import org.zuinnote.flink.office.excel.ExcelFlinkFileInputFormat

import org.zuinnote.hadoop.office.format.common._
import org.zuinnote.hadoop.office.format.common.dao._
import org.apache.flink.api.scala._

import scala.collection.mutable.ArrayBuffer
import org.scalatest.{FlatSpec, BeforeAndAfterAll, GivenWhenThen, Matchers}

class FlinkDSScalaExcelDAOIntegrationSpec extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {

private val appName: String = "example-scalaflinkexcelinput-integrationtest"
private val tmpPrefix: String = "ho-integrationtest"
private var tmpPath: java.nio.file.Path = _
private val CLUSTERNAME: String ="hcl-minicluster"
private val DFS_INPUT_DIR_NAME: String = "/input"
private val DFS_OUTPUT_DIR_NAME: String = "/output"
private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
private val DFS_INPUT_DIR : Path = new Path(DFS_INPUT_DIR_NAME)
private val DFS_OUTPUT_DIR : Path = new Path(DFS_OUTPUT_DIR_NAME)
private val NOOFDATANODES: Int =4
private var dfsCluster: MiniDFSCluster = _
private var conf: Configuration = _
private var openDecompressors = ArrayBuffer[Decompressor]();
private var flinkEnvironment: ExecutionEnvironment = _

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
	// create local Flink cluster
	flinkEnvironment = ExecutionEnvironment.createLocalEnvironment(1)
 }

  
  override def afterAll(): Unit = {
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


"The test excel file" should "be read, comments should be added to each cell and written back to a new Excel file in a different sheet" in {
	Given("Excel 2013 test file on DFS")
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy bitcoin blocks
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="excel2013test.xlsx"
    	val fileNameFullLocal=classLoader.getResource(fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)	

	When("add comments and write back to a different sheet")
	FlinkDSScalaExcelDAO.readwriteExcelDAODS(flinkEnvironment,dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME,dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
	flinkEnvironment.execute("Flink Scala DataSource/DataSink HadoopOffice read/write Excel files using DAO format (allows you define comments, formulas etc)")
	Then("Written Excel contain commments and cells are in a different sheet")
	val hocr = new HadoopOfficeReadConfiguration()
    hocr.setLocale(new Locale.Builder().setLanguageTag("us").build())
    // load Excel file
    hocr.setReadHeader(false)
    val inputFormat = new ExcelFlinkFileInputFormat(hocr)
    val excelData = flinkEnvironment.readFile(inputFormat, dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
    // check content
    val excelDataList = excelData.collect
    assert(6==excelDataList.length)
    // first row
    assert("test1"==excelDataList(0)(0).getFormattedValue())
    assert("This is a comment"==excelDataList(0)(0).getComment())
    assert(""==excelDataList(0)(0).getFormula())
    assert("A1"==excelDataList(0)(0).getAddress())
    assert("Sheet2"==excelDataList(0)(0).getSheetName())
    assert("test2"==excelDataList(0)(1).getFormattedValue())
    assert("This is a comment"==excelDataList(0)(1).getComment())
    assert(""==excelDataList(0)(1).getFormula())
    assert("B1"==excelDataList(0)(1).getAddress())
    assert("Sheet2"==excelDataList(0)(1).getSheetName())
    assert("test3"==excelDataList(0)(2).getFormattedValue())
    assert("This is a comment"==excelDataList(0)(2).getComment())
    assert(""==excelDataList(0)(2).getFormula())
    assert("C1"==excelDataList(0)(2).getAddress())
    assert("Sheet2"==excelDataList(0)(2).getSheetName())
    assert("test4"==excelDataList(0)(3).getFormattedValue())
    assert("This is a comment"==excelDataList(0)(3).getComment())
    assert(""==excelDataList(0)(3).getFormula())
    assert("D1"==excelDataList(0)(3).getAddress())
    assert("Sheet2"==excelDataList(0)(3).getSheetName())
    // second row
    assert("4"==excelDataList(1)(0).getFormattedValue())
    assert("This is a comment"==excelDataList(1)(0).getComment())
    assert(""==excelDataList(1)(0).getFormula())
    assert("A2"==excelDataList(1)(0).getAddress())
    assert("Sheet2"==excelDataList(1)(0).getSheetName())
    // third row
     assert("31/12/99"==excelDataList(2)(0).getFormattedValue())
    assert("This is a comment"==excelDataList(2)(0).getComment())
    assert(""==excelDataList(2)(0).getFormula())
    assert("A3"==excelDataList(2)(0).getAddress())
    assert("Sheet2"==excelDataList(2)(0).getSheetName())
    assert("5"==excelDataList(2)(1).getFormattedValue())
    assert("This is a comment"==excelDataList(2)(1).getComment())
    assert(""==excelDataList(2)(1).getFormula())
    assert("B3"==excelDataList(2)(1).getAddress())
    assert("Sheet2"==excelDataList(2)(1).getSheetName())
    assert("null"==excelDataList(2)(4).getFormattedValue())
    assert("This is a comment"==excelDataList(2)(4).getComment())
    assert(""==excelDataList(2)(4).getFormula())
    assert("E3"==excelDataList(2)(4).getAddress())
    assert("Sheet2"==excelDataList(2)(4).getSheetName())
    // fourth row
    assert("1"==excelDataList(3)(0).getFormattedValue())
    assert("This is a comment"==excelDataList(3)(0).getComment())
    assert(""==excelDataList(3)(0).getFormula())
    assert("A4"==excelDataList(3)(0).getAddress())
    assert("Sheet2"==excelDataList(3)(0).getSheetName())
    // fifth row
    assert("2"==excelDataList(4)(0).getFormattedValue())
    assert("This is a comment"==excelDataList(4)(0).getComment())
    assert(""==excelDataList(4)(0).getFormula())
    assert("A5"==excelDataList(4)(0).getAddress())
    assert("Sheet2"==excelDataList(4)(0).getSheetName())
    assert("6"==excelDataList(4)(1).getFormattedValue())
    assert("This is a comment"==excelDataList(4)(1).getComment())
    assert("A5*A6"==excelDataList(4)(1).getFormula())
    assert("B5"==excelDataList(4)(1).getAddress())
    assert("Sheet2"==excelDataList(4)(1).getSheetName())
    assert("10"==excelDataList(4)(2).getFormattedValue())
    assert("This is a comment"==excelDataList(4)(2).getComment())
    assert("A2+B5"==excelDataList(4)(2).getFormula())
    assert("C5"==excelDataList(4)(2).getAddress())
    assert("Sheet2"==excelDataList(4)(2).getSheetName())
     // sixth row
    assert("3"==excelDataList((5))(0).getFormattedValue())
    assert("This is a comment"==excelDataList((5))(0).getComment())
    assert(""==excelDataList((5))(0).getFormula())
    assert("A6"==excelDataList((5))(0).getAddress())
    assert("Sheet2"==excelDataList((5))(0).getSheetName())
    assert("4"==excelDataList((5))(1).getFormattedValue())
    assert("This is a comment"==excelDataList((5))(1).getComment())
    assert(""==excelDataList((5))(1).getFormula())
    assert("B6"==excelDataList((5))(1).getAddress())
    assert("Sheet2"==excelDataList((5))(1).getSheetName())
    assert("6"==excelDataList((5))(2).getFormattedValue()) // attention this is the cached value (6), in Excel you will see the result according to the formula (15)
    assert("This is a comment"==excelDataList((5))(2).getComment())
    assert("SUM(B3:B6)"==excelDataList((5))(2).getFormula())
    assert("C6"==excelDataList((5))(2).getAddress())
    assert("Sheet2"==excelDataList((5))(2).getSheetName())
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
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
import java.text.SimpleDateFormat
import java.text.DecimalFormat
import java.text.NumberFormat
import java.text.DateFormat

import org.apache.hadoop.io.compress.CodecPool
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.Decompressor
import org.apache.hadoop.io.compress.SplittableCompressionCodec
import org.apache.hadoop.io.compress.SplitCompressionInputStream



import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.apache.flink.core.fs.FileSystem.WriteMode

import org.zuinnote.hadoop.office.format.common.util._
import org.zuinnote.hadoop.office.format.common.converter._
import org.zuinnote.hadoop.office.format.common.dao._
import org.zuinnote.hadoop.office.format.common.parser._
import org.zuinnote.hadoop.office.format.common._

import org.zuinnote.flink.office.excel.ExcelFlinkTableSource

import scala.collection.mutable.ArrayBuffer
import org.scalatest.flatspec.AnyFlatSpec;
import org.scalatest._
import matchers.should._
import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen }

class FlinkTableSourceTableSinkScalaExcelIntegrationSpec extends AnyFlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {
private val appName: String = "example-flinktablesourcetablesink-integrationtest"
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
private var flinkEnvironment: ExecutionEnvironment = _
private var tableEnvironment: BatchTableEnvironment = _
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
    // create local Flink cluster
    flinkEnvironment = ExecutionEnvironment.createLocalEnvironment(1)
    tableEnvironment = BatchTableEnvironment.create(flinkEnvironment)
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


"The test excel file" should "be loaded using Flink Table Source and Written to disk using Flink Table Sink" in {
	Given("Excel 2013 test file on DFS")
	// create input directory
	dfsCluster.getFileSystem().mkdirs(DFS_INPUT_DIR)
	// copy input file to DFS
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="testsimple.xlsx"
    	val fileNameFullLocal=classLoader.getResource(fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)
    	dfsCluster.getFileSystem().copyFromLocalFile(false, false, inputFile, DFS_INPUT_DIR)	
	When("read Excel (using TableSource) and write Excel (using TableSink)")
	FlinkTableSourceTableSinkExample.readwriteExcelTableAPI(flinkEnvironment,tableEnvironment,dfsCluster.getFileSystem().getUri().toString()+DFS_INPUT_DIR_NAME,dfsCluster.getFileSystem().getUri().toString()+DFS_OUTPUT_DIR_NAME)
	flinkEnvironment.execute("HadoopOffice Flink TableSource/TableSink for Excel files Demonstration")
	Then("Excel should be written correctly to DFS")
 	val hocr: HadoopOfficeReadConfiguration = new HadoopOfficeReadConfiguration()
 	val dateFormat: SimpleDateFormat = DateFormat.getDateInstance(DateFormat.SHORT, Locale.US).asInstanceOf[SimpleDateFormat]
    val decimalFormat: DecimalFormat = NumberFormat.getInstance(Locale.GERMANY).asInstanceOf[DecimalFormat]	
    hocr.setReadHeader(true)
    hocr.setLocale(Locale.GERMANY)
    hocr.setSimpleDateFormat(dateFormat)
    hocr.setSimpleDecimalFormat(decimalFormat)
	val sourceReWritten: ExcelFlinkTableSource = ExcelFlinkTableSource.builder()
      .path(dfsCluster.getFileSystem().getUri().toString() + DFS_OUTPUT_DIR_NAME)
      .field("decimalsc1", Types.DECIMAL)
      .field("booleancolumn", Types.BOOLEAN)
      .field("datecolumn", Types.SQL_DATE)
      .field("stringcolumn", Types.STRING)
      .field("decimalp8sc3", Types.DECIMAL)
      .field("bytecolumn", Types.BYTE)
      .field("shortcolumn", Types.SHORT)
      .field("intcolumn", Types.INT)
      .field("longcolumn", Types.LONG)
      .conf(hocr)
      .build()
    tableEnvironment.registerTableSource("testsimplerewritten", sourceReWritten)

    Then("all cells should be read correctly")
    // check results of written data!!
    val testRewrittenSimpleScan = tableEnvironment.scan("testsimplerewritten")
    val testRewrittenSimpleResult = testRewrittenSimpleScan.select("*")

    val testRewrittenSimpleDS = testRewrittenSimpleResult.toDataSet[Row]
    assert(6 == testRewrittenSimpleDS.count)
    val allRows = testRewrittenSimpleDS.collect
    // check column1

    assert(new java.math.BigDecimal("1.0").compareTo(allRows(0).getField(0).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("1.5").compareTo(allRows(1).getField(0).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("3.4").compareTo(allRows(2).getField(0).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("5.5").compareTo(allRows(3).getField(0).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(null == allRows(4).getField(0))
    assert(new java.math.BigDecimal("3.4").compareTo(allRows(5).getField(0).asInstanceOf[java.math.BigDecimal]) == 0)
    // check column2
    assert(true == allRows(0).getField(1).asInstanceOf[Boolean])
    assert(false == allRows(1).getField(1).asInstanceOf[Boolean])
    assert(false == allRows(2).getField(1).asInstanceOf[Boolean])
    assert(false == allRows(3).getField(1).asInstanceOf[Boolean])
    assert(null == allRows(4).getField(1))
    assert(true == allRows(5).getField(1).asInstanceOf[Boolean])
    // check column3
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val expectedDate1 = sdf.parse("2017-01-01")
    val expectedDate2 = sdf.parse("2017-02-28")
    val expectedDate3 = sdf.parse("2000-02-29")
    val expectedDate4 = sdf.parse("2017-03-01")
    val expectedDate5 = null
    val expectedDate6 = sdf.parse("2017-03-01")
    assert(expectedDate1.compareTo(allRows(0).getField(2).asInstanceOf[java.sql.Date]) == 0)
    assert(expectedDate2.compareTo(allRows(1).getField(2).asInstanceOf[java.sql.Date]) == 0)
    assert(expectedDate3.compareTo(allRows(2).getField(2).asInstanceOf[java.sql.Date]) == 0)
    assert(expectedDate4.compareTo(allRows(3).getField(2).asInstanceOf[java.sql.Date]) == 0)
    assert(expectedDate5 == allRows(4).getField(2))
    assert(expectedDate6.compareTo(allRows(5).getField(2).asInstanceOf[java.sql.Date]) == 0)
    // check column4

    assert("This is a text" == allRows(0).getField(3).asInstanceOf[String])
    assert("Another String" == allRows(1).getField(3).asInstanceOf[String])
    assert("10" == allRows(2).getField(3).asInstanceOf[String])
    assert("test3" == allRows(3).getField(3).asInstanceOf[String])
    assert("test4" == allRows(4).getField(3).asInstanceOf[String])
    assert("test5" == allRows(5).getField(3).asInstanceOf[String])
    // check column5

    assert(new java.math.BigDecimal("10.000").compareTo(allRows(0).getField(4).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("2.334").compareTo(allRows(1).getField(4).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("4.500").compareTo(allRows(2).getField(4).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("11.000").compareTo(allRows(3).getField(4).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("100.000").compareTo(allRows(4).getField(4).asInstanceOf[java.math.BigDecimal]) == 0)
    assert(new java.math.BigDecimal("10000.500").compareTo(allRows(5).getField(4).asInstanceOf[java.math.BigDecimal]) == 0)
    // check column6
    assert(3 == allRows(0).getField(5).asInstanceOf[Byte])
    assert(5 == allRows(1).getField(5).asInstanceOf[Byte])
    assert(-100 == allRows(2).getField(5).asInstanceOf[Byte])
    assert(2 == allRows(3).getField(5).asInstanceOf[Byte])
    assert(3 == allRows(4).getField(5).asInstanceOf[Byte])
    assert(120 == allRows(5).getField(5).asInstanceOf[Byte])
    // check column7
    assert(3 == allRows(0).getField(6).asInstanceOf[Short])
    assert(4 == allRows(1).getField(6).asInstanceOf[Short])
    assert(5 == allRows(2).getField(6).asInstanceOf[Short])
    assert(250 == allRows(3).getField(6).asInstanceOf[Short])
    assert(3 == allRows(4).getField(6).asInstanceOf[Short])
    assert(100 == allRows(5).getField(6).asInstanceOf[Short])
    // check column8
    assert(100 == allRows(0).getField(7).asInstanceOf[Int])
    assert(65335 == allRows(1).getField(7).asInstanceOf[Int])
    assert(1 == allRows(2).getField(7).asInstanceOf[Int])
    assert(250 == allRows(3).getField(7).asInstanceOf[Int])
    assert(5 == allRows(4).getField(7).asInstanceOf[Int])
    assert(10000 == allRows(5).getField(7).asInstanceOf[Int])
    // check column9
    assert(65335 == allRows(0).getField(8).asInstanceOf[Long])
    assert(1 == allRows(1).getField(8).asInstanceOf[Long])
    assert(250 == allRows(2).getField(8).asInstanceOf[Long])
    assert(10 == allRows(3).getField(8).asInstanceOf[Long])
    assert(3147483647L == allRows(4).getField(8).asInstanceOf[Long])
    assert(10 == allRows(5).getField(8).asInstanceOf[Long])
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
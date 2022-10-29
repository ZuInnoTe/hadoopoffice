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


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.FileVisitResult
import java.nio.file.SimpleFileVisitor
import java.util.ArrayList
import java.util.List



import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import org.scalatest.flatspec.AnyFlatSpec;
import org.scalatest._
import matchers.should._
import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen }

class Spark3ScalaDSExcelInIntegrationSpec extends AnyFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach  with GivenWhenThen with Matchers {
 
private var sc: SparkContext = _

private val master: String = "local[2]"
private val appName: String = "example-scalasparkexcelinput-integrationtest"
private val tmpPrefix: String = "ho-integrationtest"
private var tmpPath: java.nio.file.Path = _
private val DFS_INPUT_DIR_NAME: String = "/input"
private val DFS_OUTPUT_DIR_NAME: String = "/output"
private var INPUT_DIR_FULLNAME: String = _
private var OUTPUT_DIR_FULLNAME: String = _

private val DEFAULT_OUTPUT_FILENAME: String = "part-00000"
private val DEFAULT_OUTPUT_EXCEL_FILENAME: String = "part-00000.xlsx"
private var DFS_INPUT_DIR : String = _
private var DFS_OUTPUT_DIR : String = _
private var DFS_INPUT_DIR_PATH : Path = _
private var DFS_OUTPUT_DIR_PATH : Path = _

private var conf: Configuration = _


override def beforeAll(): Unit = {
    super.beforeAll()

		// Create temporary directory for temporary files and shutdownhook
	// create temp directory
      tmpPath = Files.createTempDirectory(tmpPrefix);
      INPUT_DIR_FULLNAME = tmpPath+DFS_INPUT_DIR_NAME;
      OUTPUT_DIR_FULLNAME = tmpPath+DFS_OUTPUT_DIR_NAME;
      DFS_INPUT_DIR = "file://"+INPUT_DIR_FULLNAME;
      DFS_OUTPUT_DIR= "file://"+OUTPUT_DIR_FULLNAME;
      DFS_INPUT_DIR_PATH = new Path(DFS_INPUT_DIR);
      DFS_OUTPUT_DIR_PATH= new Path(DFS_OUTPUT_DIR);
      val inputDirFile: File = new File(INPUT_DIR_FULLNAME);
      inputDirFile.mkdirs();
      val outputDirFile: File = new File(OUTPUT_DIR_FULLNAME);
      outputDirFile.mkdirs();
      
      // create shutdown hook to remove temp files after shutdown, may need to rethink to avoid many threads are created
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
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e);
        }
        }
  }
        )
	// create local Spark cluster
 	val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName)
	sc = new SparkContext(sparkConf)
 }


  override def beforeEach() {
   // clean up folders
  cleanFolder( java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME),false);

  cleanFolder(java.nio.file.FileSystems.getDefault().getPath(OUTPUT_DIR_FULLNAME),true);
    super.beforeEach(); // To be stackable, must call super.beforeEach
  }

  override def afterEach() {
      super.afterEach(); // To be stackable, must call super.afterEach
  }

 // clean folders
  def cleanFolder(path: java.nio.file.Path, rootFolder: Boolean) {
    if (path.toFile().exists()) {
 try {
          		Files.walkFileTree(path, new SimpleFileVisitor[java.nio.file.Path]() {

            		override def visitFile(file: java.nio.file.Path,attrs: BasicFileAttributes): FileVisitResult = {
                
                		Files.delete(file)
                  
             			return FileVisitResult.CONTINUE
        			}

        		override def postVisitDirectory(dir: java.nio.file.Path, e: IOException): FileVisitResult = {
          			if (e == null) {
                    if (rootFolder) {
                      if (dir.toFile().exists) {
            			      Files.delete(dir)
                      }
                    }
            				return FileVisitResult.CONTINUE
          			}
          			throw e
        			}
        	})
      	} catch {
        case e: IOException => throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e)
    }
    }
  }

  override def afterAll(): Unit = {
   // close Spark Context
    if (sc!=null) {
	sc.stop()
    }
    super.afterAll()
}




"The test excel file" should "be converted into a 6 lines CSV" in {
	Given("Excel 2013 test file on DFS")

	// copy test excel file
	val classLoader = getClass().getClassLoader()
    	// put testdata on DFS
    	val fileName: String="excel2013test.xlsx"
    	val fileNameFullLocal=classLoader.getResource(fileName).getFile()
    	val inputFile=new Path(fileNameFullLocal)

    java.nio.file.Files.copy(java.nio.file.FileSystems.getDefault().getPath(fileNameFullLocal), java.nio.file.FileSystems.getDefault().getPath(INPUT_DIR_FULLNAME+File.separator+fileName))

	When("convert to CSV")
	val sqlContext = new SQLContext(sc)
	SparkScalaExcelInDataSource.convertToCSV(sqlContext,DFS_INPUT_DIR,DFS_OUTPUT_DIR)
	Then("CSV correspond to Excel")
	// fetch results
	val resultLines = readDefaultResults(6)
	assert(6==resultLines.size())
	assert("test1,test2,test3,test4"==resultLines.get(0))
	assert("4"==resultLines.get(1))
	assert("31/12/99,5,,,null"==resultLines.get(2))
	assert("1"==resultLines.get(3))
	assert("2,6,10"==resultLines.get(4))
	assert("3,4,15"==resultLines.get(5))
}




      /**
	* Read results from the default output directory and default outputfile name
	*
	* @param numOfRows number of rows to read
	*
	*/
     def readDefaultResults(numOfRows: Int): List[String] = {
	val result: ArrayList[String] = new ArrayList[String]()
	val defaultOutputfile = OUTPUT_DIR_FULLNAME+File.separator+DEFAULT_OUTPUT_FILENAME
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
* @param path path to the file on local file system
*
* @return InputStream from which the file content can be read
* 
* @throws java.io.Exception in case there is an issue reading the file
*
*
*/

def  openFile(path: String): InputStream = {
 	val fileIn: InputStream=new FileInputStream(path)
	// check if compressed
		return fileIn
}

}
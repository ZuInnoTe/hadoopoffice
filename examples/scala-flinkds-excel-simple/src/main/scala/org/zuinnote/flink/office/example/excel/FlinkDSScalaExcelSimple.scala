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
package org.zuinnote.flink.office.example.excel



import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.text.DecimalFormat
import java.text.NumberFormat
import java.util.Locale

import org.apache.flink.core.fs.Path

import org.zuinnote.flink.office.excel.SimpleExcelFlinkFileInputFormat
import org.zuinnote.flink.office.excel.SimpleExcelFlinkFileOutputFormat
import org.zuinnote.hadoop.office.format.common._
import org.zuinnote.hadoop.office.format.common.dao._
import org.apache.flink.api.scala._

import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration


   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the Flink DataSource / Data Sink of the HadoopOffice library
* Reads an Excel files skipping the header line (first line) using the Flink DatasSource and writes it back using Flink DataSource (without writing the header line)
* It automatically can detect the datatype in the Excel, so that you will get as an output a Flink dataset based on Flink Basic Types (e.g. string, byte, int, decimal etc.)
* This detection however, requires that the Excel is iterated twice (this can be configured, e.g. you can define to use only the first 10 lines for autodetection)
*/

object FlinkDSScalaExcelSimple {
   val MIMETYPE_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
   val MIMETYPE_XLS = "application/vnd.ms-excel";
   
   def main(args: Array[String]): Unit = {
 		val  env = ExecutionEnvironment.getExecutionEnvironment
        val params: ParameterTool = ParameterTool.fromArgs(args)

        readwriteExcelDS(env,params.get("input"),params.get("output"))
		env.execute("Flink Scala DataSource/DataSink HadoopOffice read/write Excel files using Simple format (converts Excel to basic data types)")
   }
   
   
   def readwriteExcelDS(env: ExecutionEnvironment, inputFile: String, outputFile: String): Unit = {
      val hocr = new HadoopOfficeReadConfiguration()
      hocr.setLocale(new Locale.Builder().setLanguageTag("de").build())
      // load Excel file, in order to do the conversion correctly, we need to define the format for date and decimal
	   val dateFormat: SimpleDateFormat = DateFormat.getDateInstance(DateFormat.SHORT, Locale.US).asInstanceOf[SimpleDateFormat] //important: even for non-US excel files US must be used most of the time, because this is how Excel stores them internally
	   val decimalFormat: DecimalFormat = NumberFormat.getInstance(Locale.GERMAN).asInstanceOf[DecimalFormat] 
	  val useHeader = true // the Excel file contains in the first line the header
	  // we have maxInferRows = -1 , which means we read the full Excel file first to infer the underlying schema
	  val maxInferRows = -1
	  val inputFormat = new SimpleExcelFlinkFileInputFormat(hocr, maxInferRows, useHeader, dateFormat, decimalFormat)
      val excelInData = env.readFile(inputFormat, inputFile)
      // create dataset
	  val excelData = excelInData.map(row => row) // note that each row is just an Array of Objects (Array[AnyRef]) of simple datatypes, e.g. int, long, string etc.
	  // write Excel file
	  val howc = new HadoopOfficeWriteConfiguration(new Path(outputFile).getName())
	  howc.setMimeType(MIMETYPE_XLSX)
	  val defaultSheetName = "Sheet2"
	  val header = null
	  val outputFormat = new SimpleExcelFlinkFileOutputFormat(howc, header,defaultSheetName, dateFormat, decimalFormat)
	  excelData.write(outputFormat, outputFile)
    }
}


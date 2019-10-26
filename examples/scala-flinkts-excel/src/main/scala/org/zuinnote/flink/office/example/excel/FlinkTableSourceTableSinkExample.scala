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

import java.util.Locale


import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path

import org.zuinnote.hadoop.office.format.common.util._
import org.zuinnote.hadoop.office.format.common.converter._
import org.zuinnote.hadoop.office.format.common.dao._
import org.zuinnote.hadoop.office.format.common.parser._
import org.zuinnote.hadoop.office.format.common._

import org.zuinnote.flink.office.excel.ExcelFlinkTableSource
import org.zuinnote.flink.office.excel.ExcelFlinkTableSink



import java.text.SimpleDateFormat
import java.text.DecimalFormat
import java.text.NumberFormat
import java.text.DateFormat
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the Flink TableSource/TableSink API of the HadoopOffice library
* Reads an Excel into Flink Table and writes an Excel back from the Table
*
*
*/

object FlinkTableSourceTableSinkExample {
   val MIMETYPE_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
   val MIMETYPE_XLS = "application/vnd.ms-excel";
  
   def main(args: Array[String]): Unit = {
 		val  env = ExecutionEnvironment.getExecutionEnvironment
        val params: ParameterTool = ParameterTool.fromArgs(args)
        readwriteExcelTableAPI(env,TableEnvironment.getTableEnvironment(env),params.get("input"),params.get("output"))
		env.execute("Flink Scala HadoopOffice TableSource TableSink Demonstration")
   }
   
   
   def readwriteExcelTableAPI(env: ExecutionEnvironment, tableEnv: BatchTableEnvironment,inputFile: String, outputFile: String): Unit = {

    // read using table source API
    val hocr: HadoopOfficeReadConfiguration = new HadoopOfficeReadConfiguration()
    val dateFormat: SimpleDateFormat = DateFormat.getDateInstance(DateFormat.SHORT, Locale.US).asInstanceOf[SimpleDateFormat]
    val decimalFormat: DecimalFormat = NumberFormat.getInstance(Locale.GERMANY).asInstanceOf[DecimalFormat]
    hocr.setReadHeader(true)
    hocr.setLocale(Locale.GERMANY)
    hocr.setSimpleDateFormat(dateFormat)
    hocr.setSimpleDecimalFormat(decimalFormat)
    val source: ExcelFlinkTableSource = ExcelFlinkTableSource.builder()
      .path(inputFile)
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
   	  tableEnv.registerTableSource("testsimple", source)
      val testSimpleScan = tableEnv.scan("testsimple")
      val testSimpleResult = testSimpleScan.select("*")
      // write file using TableSink
    	  val howc = new HadoopOfficeWriteConfiguration(new Path(outputFile).getName())
      howc.setMimeType(MIMETYPE_XLSX)
      howc.setLocale(Locale.GERMANY) 
      howc.setSimpleDateFormat(dateFormat)
      howc.setSimpleDecimalFormat(decimalFormat)
      val sink = new ExcelFlinkTableSink(outputFile, true, howc, "Sheet2", Some(WriteMode.NO_OVERWRITE))

    	  testSimpleResult.writeToSink(sink)
}

}

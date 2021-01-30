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
 */

package org.zuinnote.flink.office.excel

import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.sinks.{ TableSinkBase, BatchTableSink }

import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.text.DecimalFormat
import java.text.NumberFormat
import java.util.Locale
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.zuinnote.flink.office.excel.RowSimpleExcelFlinkFileOutputFormat

@deprecated
class ExcelFlinkTableSink(
  path:             String,
  useHeader:        Boolean,
  howc:             HadoopOfficeWriteConfiguration,
  defaultSheetName: String                         = "Sheet1",
  writeMode:        Option[WriteMode]              = Some(WriteMode.NO_OVERWRITE)) extends TableSinkBase[Row] with BatchTableSink[Row] {






/**
*
* Flink 1.11
**/
 def consumeDataSet(dataSet: DataSet[Row]): DataSink[Row] = {
var outputFormat: RowSimpleExcelFlinkFileOutputFormat = null;
if (useHeader) {
  outputFormat = new RowSimpleExcelFlinkFileOutputFormat(howc, this.getFieldNames, defaultSheetName)
} else {
  outputFormat = new RowSimpleExcelFlinkFileOutputFormat(howc, null, defaultSheetName)
}
outputFormat.setOutputFilePath(new Path(path))
outputFormat.setWriteMode(writeMode.get)

return dataSet.write(outputFormat, path).name(TableConnectorUtils.generateRuntimeName(this.getClass, getFieldNames))

}


/***
* Following is for Flink < 1.10
*
*/
/***
   * Writes a dataset in Excel format. If useHeader is activated then it adds the field names of the dataset as the first row
   *
   * @param dataSet dataset to write
   *
   */
   def emitDataSet(dataSet: DataSet[Row]): Unit = {

    var outputFormat: RowSimpleExcelFlinkFileOutputFormat = null;
    if (useHeader) {
      outputFormat = new RowSimpleExcelFlinkFileOutputFormat(howc, this.getFieldNames, defaultSheetName)
    } else {
      outputFormat = new RowSimpleExcelFlinkFileOutputFormat(howc, null, defaultSheetName)
    }
    outputFormat.setOutputFilePath(new Path(path))
    outputFormat.setWriteMode(writeMode.get)

    dataSet.write(outputFormat, path).name(this.getClass + getFieldNames.mkString(","))
  }

  override protected def copy: TableSinkBase[Row] = {
    new ExcelFlinkTableSink(path, useHeader, howc, defaultSheetName,writeMode)
  }

  override def getOutputType: TypeInformation[Row] = {
    new RowTypeInfo(getFieldTypes: _*)
  }
}

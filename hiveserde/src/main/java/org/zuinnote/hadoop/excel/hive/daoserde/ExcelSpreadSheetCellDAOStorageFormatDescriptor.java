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
package org.zuinnote.hadoop.excel.hive.daoserde;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.io.AbstractStorageFormatDescriptor;
import org.zuinnote.hadoop.excel.hive.outputformat.HiveExcelCellFileOutputFormat;
import org.zuinnote.hadoop.office.format.mapred.ExcelFileInputFormat;

/**
 * @author jornfranke
 *
 */
public class ExcelSpreadSheetCellDAOStorageFormatDescriptor  extends AbstractStorageFormatDescriptor  {

	@Override
	public Set<String> getNames() {
		HashSet<String> result = new HashSet<>();
	    result.add("EXCELSPREADSHEETCELLDAOFILE");
	    return result;
	}

	@Override
	public String getInputFormat() {
		return ExcelFileInputFormat.class.getName();
	}

	@Override
	public String getOutputFormat() {
		return HiveExcelCellFileOutputFormat.class.getName();
	}
	
	@Override
	public String getSerde() {
		return ExcelSpreadSheetCellDAOSerde.class.getName();
	}

}
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
package org.zuinnote.flink.office.excel;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.types.Row;
import org.zuinnote.flink.office.AbstractSpreadSheetFlinkFileOutputFormat;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO;

/**
 *
 *
 */
public class RowSimpleExcelFlinkFileOutputFormat extends AbstractSpreadSheetFlinkFileOutputFormat<Row> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 501902887879833189L;
	private static final Log LOG = LogFactory.getLog(RowSimpleExcelFlinkFileOutputFormat.class.getName());
	private ExcelConverterSimpleSpreadSheetCellDAO converter;
	private String defaultSheetName;
	private int rowNumSimple;

	public RowSimpleExcelFlinkFileOutputFormat(HadoopOfficeWriteConfiguration howc, String[] header,
			String defaultSheetName, SimpleDateFormat dateFormat, DecimalFormat decimalFormat) {
		super(howc, header, defaultSheetName);
		this.converter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat, decimalFormat);
		this.defaultSheetName = defaultSheetName;
		this.rowNumSimple = 0;
		if ((header != null) && (header.length > 0)) {
			this.rowNumSimple++;
		}
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		// TODO Auto-generated method stub
		if (record != null) {
			Object[] simpleRow = new Object[record.getArity()];
			for (int i = 0; i < simpleRow.length; i++) {
				simpleRow[i] = record.getField(i);
			}
			this.writeRow(converter.getSpreadSheetCellDAOfromSimpleDataType(simpleRow, this.defaultSheetName,
					this.rowNumSimple));
		}
		this.rowNumSimple++;
	}

}

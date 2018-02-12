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
package org.zuinnote.flink.office.excel;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zuinnote.flink.office.AbstractSpreadSheetFlinkFileOutputFormat;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;

/**
 * @author jornfranke
 *
 */
public class SimpleExcelFlinkFileOutputFormat extends AbstractSpreadSheetFlinkFileOutputFormat<Object[]> {
	
	

	private static final Log LOG = LogFactory.getLog(SimpleExcelFlinkFileOutputFormat.class.getName());
	/**
	 * 
	 */
	private static final long serialVersionUID = 8528766434712667829L;
	private ExcelConverterSimpleSpreadSheetCellDAO converter;
	private String defaultSheetName;
	private int rowNum;
	
	public SimpleExcelFlinkFileOutputFormat(HadoopOfficeWriteConfiguration howc, String[] header, String defaultSheetName, ExcelConverterSimpleSpreadSheetCellDAO converter) {
		super(howc,header,defaultSheetName);
		this.converter=converter;
		this.defaultSheetName=defaultSheetName;
		this.rowNum=0;
		if ((header!=null) && (header.length>0)) {
			this.rowNum++;
		}
	}

	@Override
	public void writeRecord(Object[] record) throws IOException {
		this.writeRow(converter.getSpreadSheetCellDAOfromSimpleDataType(record, this.defaultSheetName, rowNum));
		this.rowNum++;
		
	}

}

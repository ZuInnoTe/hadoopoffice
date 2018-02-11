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
import org.apache.flink.api.common.io.FileOutputFormat;
import org.zuinnote.flink.office.AbstractSpreadSheetFlinkFileOutputFormat;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;

/**
 * @author jornfranke
 *
 */
public class ExcelFlinkFileOutputFormat extends AbstractSpreadSheetFlinkFileOutputFormat<SpreadSheetCellDAO[]> {
	
	
	public ExcelFlinkFileOutputFormat(HadoopOfficeWriteConfiguration howc) {
		super(howc);
	}





	private static final Log LOG = LogFactory.getLog(ExcelFlinkFileOutputFormat.class.getName());
	/**
	 * 
	 */
	private static final long serialVersionUID = -2130587077135726939L;

	
   /*
    * Write an excel row 
    * 
    * @param record Row to write
    * 
    * @see org.apache.flink.api.common.io.OutputFormat#writeRecord(java.lang.Object)
    */


	@Override
	public void writeRecord(SpreadSheetCellDAO[] record) throws IOException {

		try {
			this.getOfficeWriter().write(record);
		} catch (OfficeWriterException e) {
			LOG.error(e);
		}
		
	}
	


}

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
import org.zuinnote.flink.office.AbstractSpreadSheetFlinkFileInputFormat;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

/**
 *
 *
 */
public class ExcelFlinkFileInputFormat extends AbstractSpreadSheetFlinkFileInputFormat<SpreadSheetCellDAO[]> {
	private static final Log LOG = LogFactory.getLog(ExcelFlinkFileInputFormat.class.getName());
	
	
	public ExcelFlinkFileInputFormat(HadoopOfficeReadConfiguration hocr, boolean useHeader) {
		super(hocr,useHeader);
		hocr.setMimeType(AbstractSpreadSheetFlinkFileInputFormat.MIMETYPE_EXCEL);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 547742687992385083L;


	@Override
	public SpreadSheetCellDAO[] nextRecord(SpreadSheetCellDAO[] reuse) throws IOException {
		return (SpreadSheetCellDAO[]) this.readNextRow();
	}





	
	
}

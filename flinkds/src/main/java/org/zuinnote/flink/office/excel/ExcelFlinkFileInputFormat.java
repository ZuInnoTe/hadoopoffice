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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.zuinnote.flink.office.AbstractSpreadSheetFlinkFileInputFormat;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

/**
 *
 *
 */
public class ExcelFlinkFileInputFormat extends AbstractSpreadSheetFlinkFileInputFormat<SpreadSheetCellDAO[]> implements CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>>{
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



	/***
	 * 
	 * Reopens the stream. Note that it still needs to parse the full Excel file,
	 * but it resumes for nextRecord from the current sheet and row number
	 * 
	 * @param split
	 * @param state
	 * @throws IOException
	 */

	@Override
	public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
		this.open(split);
		this.getOfficeReader().getCurrentParser().setCurrentSheet(state.f0);
		this.getOfficeReader().getCurrentParser().setCurrentRow(state.f1);

	}

	@Override
	public Tuple2<Long, Long> getCurrentState() throws IOException {
		return new Tuple2<>(this.getOfficeReader().getCurrentParser().getCurrentSheet(), this.getOfficeReader().getCurrentParser().getCurrentRow());
	}

	
	
}

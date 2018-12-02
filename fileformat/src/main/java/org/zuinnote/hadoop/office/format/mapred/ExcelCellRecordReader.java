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

package org.zuinnote.hadoop.office.format.mapred;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;

/* ExcelRecordReader reads cells from Excel
*
* You can specify the following options:
* See AbstractTableDocumentRecordReader
*
*
*/
public class ExcelCellRecordReader extends AbstractSpreadSheetDocumentRecordReader<Text,SpreadSheetCellDAO>{

	private static final Log LOG = LogFactory.getLog(ExcelCellRecordReader.class.getName());
	private FileSplit split;
	private Object[] objectArray;
	private int objectArrayPos;
	
	public ExcelCellRecordReader(FileSplit split, JobConf job, Reporter reporter)
			throws IOException, FormatNotUnderstoodException, GeneralSecurityException {
		super(split, job, reporter);
		this.split=split;
		this.objectArrayPos=0;
	}

	@Override
	public boolean next(Text key, SpreadSheetCellDAO value) throws IOException {
		if (!(this.getOfficeReader().getFiltered())) {
			return false;
		}
		while ((this.objectArray==null) || (this.objectArray.length==0) || (this.objectArrayPos==this.objectArray.length) || (this.objectArray[this.objectArrayPos]==null)) {
			// check if we need to skip non-existing cells
			if ((this.objectArray!=null) && (this.objectArrayPos<this.objectArray.length) && (this.objectArray[this.objectArrayPos]==null)) {
				this.objectArrayPos++;
			}
			else { // we need to load a  new row
				this.objectArray = this.getOfficeReader().getNext();
				this.objectArrayPos=0;
				if (objectArray==null) {
					return false; // no more to read
				}
			}
		}
		SpreadSheetCellDAO currentCell = (SpreadSheetCellDAO) this.objectArray[this.objectArrayPos++];
		key.set("["+this.split.getPath().getName()+"]"+this.getOfficeReader().getCurrentSheetName()+"!A"+this.getOfficeReader().getCurrentRow());
		value.set(currentCell);
		return true;	
	}

	@Override
	public Text createKey() {
		return new Text("");	
	}

	@Override
	public SpreadSheetCellDAO createValue() {
		return new SpreadSheetCellDAO();
	}

}

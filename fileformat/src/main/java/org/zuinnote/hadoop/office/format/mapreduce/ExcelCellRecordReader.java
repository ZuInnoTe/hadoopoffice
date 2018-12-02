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

package org.zuinnote.hadoop.office.format.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

public class ExcelCellRecordReader extends AbstractSpreadSheetDocumentRecordReader<Text,SpreadSheetCellDAO> {
	private static final Log LOG = LogFactory.getLog(ExcelCellRecordReader.class.getName());
	private FileSplit split;
	private Text currentKey=new Text("");
	private SpreadSheetCellDAO currentValue=new SpreadSheetCellDAO();
	private Object[] objectArray;
	private int objectArrayPos;
	
	
	public ExcelCellRecordReader(Configuration conf, FileSplit split) {
		super(conf);
		 LOG.debug("Initalizing ExcelRecordReader");
		 this.split=split;
	}

	/**
	*
	* Read row from Office document. If document does not match a defined metadata filter then it returns no rows. If no metadata filter is defined or document matches metadata filter then it returns rows, if available in the document/selected sheet
	*
	* @return true if next more rows are available, false if not
	*/
	@Override
	public boolean nextKeyValue() throws IOException {
		
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
		this.currentKey.set("["+this.split.getPath().getName()+"]"+this.getOfficeReader().getCurrentSheetName()+"!A"+this.getOfficeReader().getCurrentRow());
		this.currentValue.set(currentCell);
		return true;
	}

	/**
	*
	*  get current key after calling next()
	*
	* @return key is a text containing a reference for the SpreadSheet (e.g. [name.xlsx]Sheet1!A1)
	*/
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.currentKey;
	}

	/**
	*
	*  get current value after calling next()
	*
	* @return is an object of type SpreadSheetCellDAO
	*/
	@Override
	public SpreadSheetCellDAO getCurrentValue() throws IOException, InterruptedException {
		return this.currentValue;
	}

}

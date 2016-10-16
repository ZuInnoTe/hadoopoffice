/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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

package org.zuinnote.hadoop.office.format.dao;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
* This DAO represents a spreadsheet cell
*/

public class SpreadSheetCellDAO implements Writable {
private String value;
private String comment;
private String formula;
private String address;

public SpreadSheetCellDAO() {
	this.value="";
	this.comment="";
	this.formula="";
	this.address="";
}


public SpreadSheetCellDAO(String value, String comment, String formula, String address) {
	this.value=value;
	this.comment=comment;
	this.formula=formula;
	this.address=address;
}

public String getValue() {
	return this.value;
}

public String getComment() {
	return this.comment;
}

public String getFormula() {
	return this.formula;
}

public String getAddress() {
	return this.address;
}


public void set(SpreadSheetCellDAO newSpreadSheetCellDAO) {
	this.value=newSpreadSheetCellDAO.getValue();
        this.comment=newSpreadSheetCellDAO.getComment();
	this.formula=newSpreadSheetCellDAO.getFormula();
	this.address=newSpreadSheetCellDAO.getAddress();
}


/** Writable **/

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("readFields unsupported");
}

}

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

package org.zuinnote.hadoop.office.format.common.parser;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;
/*
*
* This interface is implemented by all parsers
*
*/

public interface OfficeReaderParserInterface {

public void parse(InputStream inputStream) throws FormatNotUnderstoodException;
public long getCurrentRow();
public void setCurrentRow(long row);
public long getCurrentSheet();
public void setCurrentSheet(long sheet);
public String getCurrentSheetName();
public boolean addLinkedWorkbook(String name, InputStream inputStream,String password) throws FormatNotUnderstoodException;
public List<String> getLinkedWorkbooks();
public Object[] getNext();
public boolean getFiltered();
public void close() throws IOException;

}

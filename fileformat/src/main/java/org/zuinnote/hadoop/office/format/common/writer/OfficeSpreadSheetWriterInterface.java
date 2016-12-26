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

package org.zuinnote.hadoop.office.format.common.writer;

import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;


import java.util.Properties;


import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

public interface OfficeSpreadSheetWriterInterface {

public void create(OutputStream osStream, Map<String,InputStream> linkedWorkbooks) throws IOException,FormatNotUnderstoodException;

public void write(Object newDAO) throws InvalidCellSpecificationException,ObjectNotSupportedException;

public void finalizeWrite() throws IOException;

}

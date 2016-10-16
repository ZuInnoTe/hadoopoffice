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

package org.zuinnote.hadoop.office.format;


import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/* ExcelRecordReader reads rows from Excel
*
* You can specify the following options:
* "io.file.buffer.size": Size of io Buffer. Defaults to 64K
* "hadoopoffice.msexcel.locale": locale to use, defaults to empty string (=use default locale)
*
*
*/


public class ExcelRecordReader extends AbstractTableDocumentRecordReader<LongWritable,TextArrayWritable> {
private static final Log LOG = LogFactory.getLog(ExcelRecordReader.class.getName());


/**
*
* Create an empty key
*
* @return key
*/
public abstract K createKey();

/**
*
* Create an empty value
*
* @return value
*/
public abstract V createValue();



/**
*
* Read row from Office document
*
* @return true if next more rows are available, false if not
*/
public abstract boolean next(K key, V value) throws IOException;



}

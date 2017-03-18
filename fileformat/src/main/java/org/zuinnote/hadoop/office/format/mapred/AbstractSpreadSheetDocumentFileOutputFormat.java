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

package org.zuinnote.hadoop.office.format.mapred;


import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapred.RecordWriter;



import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;


/* The input format will return an array of strings that it reads per "row" from the source formats */

public abstract class AbstractSpreadSheetDocumentFileOutputFormat  extends FileOutputFormat<NullWritable,SpreadSheetCellDAO> {

@Override
public abstract RecordWriter<NullWritable,SpreadSheetCellDAO> getRecordWriter(FileSystem ignored, JobConf conf, String name, Progressable progress) throws IOException;
	


}

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
import java.io.DataOutputStream;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Progressable;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;


/* The input format will return an array of strings that it reads per "row" from the source formats */

public abstract class AbstractSpreadSheetDocumentFileOutputFormat  extends FileOutputFormat<NullWritable,SpreadSheetCellDAO> {
private static final Log LOG = LogFactory.getLog(AbstractSpreadSheetDocumentFileOutputFormat.class.getName());


public abstract RecordWriter<NullWritable,SpreadSheetCellDAO> getRecordWriter(FileSystem ignored, JobConf conf, String name, Progressable progress) throws IOException;
	

/*
* Creates for the file to be written and outputstream and takes - depending on the configuration - take of compression. Set for compression the following options:
* mapreduce.output.fileoutputformat.compress true/false
* mapreduce.output.fileoutputformat.compress.codec java class of compression codec
*
* Note that some formats may use already internal compression so that additional compression does not lead to many benefits
*
* @param conf Configuration of Job
* @param file file to be written
*
* @return outputstream of the file
*
*/

public DataOutputStream getDataOutputStream(JobConf conf, Path file, Progressable progress) throws IOException {
if (getCompressOutput(conf)==false) { // uncompressed
	FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, progress);
	return fileOut;
} else { // compressed (note partially adapted from TextOutputFormat)
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(conf, GzipCodec.class); // Gzip is default if no other has been selected
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      // provide proper file extension
      Path compressedFile = file.suffix(codec.getDefaultExtension());
      // build the filename including the extension
      FileSystem fs = compressedFile.getFileSystem(conf);
      FSDataOutputStream realFileOut = fs.create(compressedFile, progress);
      return new DataOutputStream(codec.createOutputStream(realFileOut));
}
}

}

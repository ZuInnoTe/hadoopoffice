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
package org.zuinnote.hadoop.office.format.common;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

/** This class servers as a reader for a file from any file system supported by Hadoop. It is suppposed to be used only for loading related files to a main file (e.g. linked workbook), but not the main file itself (this is done via the normal file format mechanism).
 **/

public class HadoopFileReader {
private Configuration conf;


private HadoopFileReader() {
}

/**
* Create a new HadoopFileReader
*
* @param conf filesystem configuration, can be taken over from the current job where the inputformat is used
*
*/

public HadoopFileReader(Configuration conf) {
	this.conf=conf;
}

/*
* Opens a file using the Hadoop API
*
* @param path path to the file, e.g. file://path/to/file for a local file or hdfs://path/to/file for HDFS file. All filesystem configured for Hadoop can be used
*
* @return InputStream from which the file content can be read
* 
* @throws java.io.Exception in case there is an issue reading the file
*
*
*/

public InputStream openFile(Path path) throws IOException {
        FileSystem fs = FileSystem.get(this.conf);
	return fs.open(path);

}

}

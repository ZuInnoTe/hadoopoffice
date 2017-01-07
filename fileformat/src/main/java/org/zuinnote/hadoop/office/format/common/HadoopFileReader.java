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
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;


import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/** This class servers as a reader for a file from any file system supported by Hadoop. It is suppposed to be used only for loading related files to a main file (e.g. linked workbook), but not the main file itself (this is done via the normal file format mechanism).
 **/

public class HadoopFileReader {
private static final Log LOG = LogFactory.getLog(HadoopFileReader.class.getName());
private CompressionCodec codec;
private CompressionCodecFactory compressionCodecs = null;
private Configuration conf;
private ArrayList<Decompressor> openDecompressors;
private FileSystem fs;

private HadoopFileReader() {
}

/**
* Create a new HadoopFileReader
*
* @param conf filesystem configuration, can be taken over from the current job where the inputformat is used
*
*/

public HadoopFileReader(Configuration conf) throws IOException {
	this.conf=conf;
	this.compressionCodecs=  new CompressionCodecFactory(conf);
	this.openDecompressors = new ArrayList<Decompressor>();
	this.fs = FileSystem.get(this.conf);
	
}

/*
* Opens a file using the Hadoop API. It supports uncompressed and compressed files.
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
        CompressionCodec codec=compressionCodecs.getCodec(path);
 	FSDataInputStream fileIn=fs.open(path);
	// check if compressed
	if (codec==null) { // uncompressed
	LOG.debug("Reading from an uncompressed file \""+path+"\"");
		return fileIn;
	} else { // compressed
		Decompressor decompressor = CodecPool.getDecompressor(codec);
		this.openDecompressors.add(decompressor); // to be returned later using close
		if (codec instanceof SplittableCompressionCodec) {
			LOG.debug("Reading from a compressed file \""+path+"\" with splittable compression codec");
			long end = fs.getFileStatus(path).getLen(); 
        		final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, 0, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
					return cIn;
      		} else {
			LOG.debug("Reading from a compressed file \""+path+"\" with non-splittable compression codec");
        		return codec.createInputStream(fileIn,decompressor);
      		}
	}
}

/*
* Closes the reader. Note that you are on your own to close related inputStreams.
*
*/
public void close() throws IOException {
 for (Decompressor currentDecompressor: this.openDecompressors) {
	if (currentDecompressor!=null) {
		 CodecPool.returnDecompressor(currentDecompressor);
	}
 }
 // never close the filesystem. Causes issues with Spark
}

/*
* Loads linked workbooks as InputStreams
* 
* @param fileNames List of filenames (full URI/path) to load
*
* @return a Map of filenames (without path!) with associated InputStreams
*
* @throws java.io.IOException in case of issues loading a file
*
*/

public Map<String,InputStream> loadLinkedWorkbooks(String[] fileNames) throws IOException {
	HashMap<String,InputStream> result = new HashMap<String,InputStream>();
	if (fileNames==null) return result;
	for (String currentFile: fileNames) {
		Path currentPath=new Path(currentFile);
		InputStream currentInputStream = openFile(currentPath);
		result.put(currentPath.getName(), currentInputStream); 
	}
	return result;
}

}

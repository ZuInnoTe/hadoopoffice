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

import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;

import java.util.Locale;
import java.util.Locale.Builder;

import org.apache.hadoop.io.BytesWritable; 

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.parser.*;




/**
* This abstract reader implements some generic functionality (e.g. buffers etc.) for reading tables from various formats
*
*
*/

public abstract class AbstractTableDocumentRecordReader<K,V> implements RecordReader<K,V> {
private static final Log LOG = LogFactory.getLog(AbstractTableDocumentRecordReader.class.getName());
private static final String CONF_BUFFERSIZE="io.file.buffer.size";
private static final String CONF_MIMETYPE="hadoopoffice.mimeType";
private static final String CONF_SHEETS="hadoopoffice.sheets";
private static final String CONF_LOCALE="hadoopoffice.locale.bcp47";
private static final String CONF_LINKEDWB="hadoopoffice.linkedworkbooks";
private static final int DEFAULT_BUFFERSIZE=64*1024;
private static final String DEFAULT_MIMETYPE="";
private static final String DEFAULT_LOCALE="";
private static final String DEFAULT_SHEETS="";
private static final String DEFAULT_LINKEDWB="";

private int bufferSize=DEFAULT_BUFFERSIZE;
private String mimeType=null;
private String localeStrBCP47=null;
private String sheets=null;
private Locale locale=null;
private OfficeReader officeReader=null;

private CompressionCodec codec;
private CompressionCodecFactory compressionCodecs = null;
private Decompressor decompressor;
private Configuration conf;
private long start;
private long pos;
private long end;
private final Seekable filePosition;
private FSDataInputStream fileIn;

/**
* Creates an Abstract Record Reader for tables from various document formats
* @param split Split to use (assumed to be a file split)
* @param job Configuration:
* io.file.buffer.size: Size of in-memory  specified in the given Configuration. If io.file.buffer.size is not specified the default buffersize will be used. 
* hadoopoffice.mimeType: Mimetype of the document
* hadoopoffice.locale: Locale of the document (e.g. needed for interpreting spreadsheets) in the BCP47 format (cf. https://tools.ietf.org/html/bcp47). If not specified then default system locale will be used.
* hadoopoffice.sheets: A ":" separated list of sheets to be read. If not specified then all sheets will be read one after the other
* @param reporter Reporter
* hadoopoffice.linkedworkbooks: A "[file1]:[filex]" separated list of file names (e.g. HDFS Urls) that are linked to this workbook. Example: "[hdfs://user/test/input/linkedwb1.xls]:[hdfs://user/test/input/linkedwb2.xlsx]"If not specified then it is assumed that no links exist to other files. Linked Workbooks will be processed together with the main workbook on one node and thus it should be avoided to have a lot of linked workbooks.
*
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop

*
*/
public AbstractTableDocumentRecordReader(FileSplit split, JobConf job, Reporter reporter) throws IOException,FormatNotUnderstoodException {
 	// parse configuration
     this.conf=job;	
     this.bufferSize=conf.getInt(this.CONF_BUFFERSIZE,this.DEFAULT_BUFFERSIZE);
     this.mimeType=conf.get(this.CONF_MIMETYPE,this.DEFAULT_MIMETYPE);
     this.sheets=conf.get(this.CONF_SHEETS,this.DEFAULT_SHEETS);
     this.localeStrBCP47=conf.get(this.CONF_LOCALE, this.DEFAULT_LOCALE);
     if (!("".equals(localeStrBCP47))) { // create locale
	this.locale=new Locale.Builder().setLanguageTag(this.localeStrBCP47).build();
      }
 // Initialize start and end of split
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
     compressionCodecs = new CompressionCodecFactory(job);
    codec = new CompressionCodecFactory(job).getCodec(file);
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    // open stream
      if (isCompressedInput()) { // decompress
      	decompressor = CodecPool.getDecompressor(codec);
      	if (codec instanceof SplittableCompressionCodec) {
		
        	final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, start, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
		officeReader = new OfficeReader(cIn, this.bufferSize, this.mimeType, this.sheets, this.locale);  
		start = cIn.getAdjustedStart();
       		end = cIn.getAdjustedEnd();
        	filePosition = cIn; // take pos from compressed stream
      } else {
	officeReader = new OfficeReader(codec.createInputStream(fileIn,decompressor), this.bufferSize, this.mimeType, this.sheets, this.locale);
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
	officeReader = new OfficeReader(fileIn, this.bufferSize, this.mimeType,this.sheets, this.locale);
      filePosition = fileIn;
    }
    // initialize reader
this.pos=start;
	this.officeReader.parse();
}


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


/*
* Get the office reader for the current file
*
* @return OfficeReader for the current file
*
*/

public OfficeReader getOfficeReader() {
	return this.officeReader;
}

/**
* Get the current file position in a compressed or uncompressed file.
*
* @return file position
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
*
*/

public long getFilePosition() throws IOException {
	return  filePosition.getPos();
}

/**
* Get the end of file
*
* @return end of file position
*
*/

public long getEnd() {
	return end;
}

/*
* Returns how much of the file has been processed in terms of bytes
*
* @return progress percentage
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
*
*/

public synchronized float getProgress() throws IOException {
if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
}

/*
* Determines if the input is compressed or not
*
* @return true if compressed, false if not
*/
private boolean  isCompressedInput() {
    return (codec != null);
  }

/*
* Get current position in the stream
*
* @return position
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
*
*/

public  synchronized long getPos() throws IOException {
	return filePosition.getPos();
}

/*
* Clean up InputStream and Decompressor after use
*
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
*
*/

public synchronized void  close() throws IOException {
try {
    if (officeReader!=null) {
	officeReader.close();
     }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
}


}

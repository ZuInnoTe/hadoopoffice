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

package org.zuinnote.hadoop.office.format.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;

import java.security.GeneralSecurityException;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.HadoopFileReader;
import org.zuinnote.hadoop.office.format.common.HadoopUtil;
import org.zuinnote.hadoop.office.format.common.OfficeReader;
import org.zuinnote.hadoop.office.format.common.parser.*;




/**
* This abstract reader implements some generic functionality (e.g. buffers etc.) for reading tables from various formats
*
*
*/

public abstract class AbstractSpreadSheetDocumentRecordReader<K,V> extends RecordReader<K,V> {
private static final Log LOG = LogFactory.getLog(AbstractSpreadSheetDocumentRecordReader.class.getName());
public static final String CONF_MIMETYPE="hadoopoffice.read.mimeType";
public static final String CONF_SHEETS="hadoopoffice.read.sheets";
public static final String CONF_LOCALE="hadoopoffice.read.locale.bcp47";
public static final String CONF_LINKEDWB="hadoopoffice.read.linkedworkbooks";
public static final String CONF_IGNOREMISSINGWB="hadoopoffice.read.ignoremissinglinkedworkbooks";
public static final String CONF_DECRYPT="hadoopoffice.read.security.crypt.password";
public static final String CONF_DECRYPTLINKEDWBBASE="hadoopoffice.read.security.crypt.linkedworkbooks.";
public static final String CONF_FILTERMETADATA = "hadoopoffice.read.filter.metadata."; // base: all these properties (e.g. hadoopoffice.read.filter.metadata.author) will be handed over to the corresponding reader which does the filtering!
public static final String DEFAULT_MIMETYPE="";
public static final String DEFAULT_LOCALE="";
public static final String DEFAULT_SHEETS="";
public static final boolean DEFAULT_LINKEDWB=false;
public static final boolean DEFAULT_IGNOREMISSINGLINKEDWB=false;


private String mimeType=null;
private String localeStrBCP47=null;
private String sheets=null;
private Locale locale=null;
private boolean readLinkedWorkbooks=this.DEFAULT_LINKEDWB;
private boolean ignoreMissingLinkedWorkbooks=this.DEFAULT_IGNOREMISSINGLINKEDWB;
private OfficeReader officeReader=null;
private String password=null;
private String linkedWBpassword=null;
private Map<String,String> metadataFilter;
private Map<String,String> linkedWBCredentialMap;

private CompressionCodec codec;
private CompressionCodecFactory compressionCodecs = null;
private Decompressor decompressor;
private Configuration conf;
private long start;
private long pos;
private long end;
private Seekable filePosition;
private FSDataInputStream fileIn;
private HadoopFileReader currentHFR;
private FileSystem fs;


/**
* Creates an Abstract Record Reader for tables from various document formats
* @param conf Configuration:
* hadoopoffice.read.mimeType: Mimetype of the document
* hadoopoffice.read.locale: Locale of the document (e.g. needed for interpreting spreadsheets) in the BCP47 format (cf. https://tools.ietf.org/html/bcp47). If not specified then default system locale will be used.
* hadoopoffice.read.sheets: A ":" separated list of sheets to be read. If not specified then all sheets will be read one after the other
* hadoopoffice.read.linkedworkbooks: true if linkedworkbooks should be fetched. They must be in the same folder as the main workbook. Linked Workbooks will be processed together with the main workbook on one node and thus it should be avoided to have a lot of linked workbooks. It does only read the linked workbooks that are directly linked to the main workbook. Default: false
* hadoopoffice.read.ignoremissinglinkedworkbooks: true if missing linked workbooks should be ignored. Default: false
* hadoopoffice.read.security.crypt.password: if set then hadoopoffice will try to decrypt the file
* hadoopoffice.read.security.crypt.linkedworkbooks.*: if set then hadoopoffice will try to decrypt all the linked workbooks where a password has been specified. If no password is specified then it is assumed that the linked workbook is not encrypted. Example: Property key for file "linkedworkbook1.xlsx" is  "hadoopoffice.read.security.crypt.linkedworkbooks.linkedworkbook1.xslx". Value is the password. You must not include path or protocol information in the filename 
* hadoopoffice.read.filter.metadata: filters documents according to metadata. For example, hadoopoffice.read.filter.metadata.author will filter by author and the filter defined as value. Filtering is done by the parser and it is recommended that it supports regular expression for filtering, but this is up to the parser!

*
*
*/
public AbstractSpreadSheetDocumentRecordReader(Configuration conf) {
 	// parse configuration
     this.conf=conf;	
     this.mimeType=conf.get(this.CONF_MIMETYPE,this.DEFAULT_MIMETYPE);
     this.sheets=conf.get(this.CONF_SHEETS,this.DEFAULT_SHEETS);
     this.localeStrBCP47=conf.get(this.CONF_LOCALE, this.DEFAULT_LOCALE);
     if (!("".equals(localeStrBCP47))) { // create locale
	this.locale=new Locale.Builder().setLanguageTag(this.localeStrBCP47).build();
      }
      this.readLinkedWorkbooks=conf.getBoolean(this.CONF_LINKEDWB,this.DEFAULT_LINKEDWB);
      this.ignoreMissingLinkedWorkbooks=conf.getBoolean(CONF_IGNOREMISSINGWB,this.DEFAULT_IGNOREMISSINGLINKEDWB);
      this.password=conf.get(this.CONF_DECRYPT); // null if no password is set
      this.linkedWBpassword=conf.get(CONF_DECRYPTLINKEDWBBASE); // null if no password is set
      this.metadataFilter=HadoopUtil.parsePropertiesFromBase(conf,this.CONF_FILTERMETADATA);
      this.linkedWBCredentialMap=HadoopUtil.parsePropertiesFromBase(conf,this.CONF_DECRYPTLINKEDWBBASE);
}


/**
* Initializes reader
* @param split Split to use (assumed to be a file split)
* @param context context of the job
*
*
* @throws java.io.IOException in case of errors reading from the filestream provided by Hadoop
* @throws java.lang.InterruptedException in case of thread interruption
*
*/
@Override
public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
try {
   FileSplit fSplit = (FileSplit)split;
 // Initialize start and end of split
    start = fSplit.getStart();
    end = start + fSplit.getLength();
    final Path file = fSplit.getPath();
     compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
    codec = compressionCodecs.getCodec(file);
     fs = file.getFileSystem(context.getConfiguration());
    fileIn = fs.open(file);
    // open stream
      if (isCompressedInput()) { // decompress
      	decompressor = CodecPool.getDecompressor(codec);
      	if (codec instanceof SplittableCompressionCodec) {
		LOG.debug("Reading from a compressed file \""+file+"\" with splittable compression codec");
        	final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, start, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
		officeReader = new OfficeReader(cIn, this.mimeType, this.sheets, this.locale,this.ignoreMissingLinkedWorkbooks,file.getName(),this.password,this.metadataFilter);  
		start = cIn.getAdjustedStart();
       		end = cIn.getAdjustedEnd();
        	filePosition = cIn; // take pos from compressed stream
      } else {
	LOG.debug("Reading from a compressed file \""+file+"\" with non-splittable compression codec");
	officeReader = new OfficeReader(codec.createInputStream(fileIn,decompressor), this.mimeType, this.sheets, this.locale, this.ignoreMissingLinkedWorkbooks,file.getName(),this.password,this.metadataFilter);
        filePosition = fileIn;
      }
    } else {
	LOG.debug("Reading from an uncompressed file \""+file+"\"");
      fileIn.seek(start);
	officeReader = new OfficeReader(fileIn, this.mimeType,this.sheets, this.locale,this.ignoreMissingLinkedWorkbooks,file.getName(),this.password,this.metadataFilter);  
      filePosition = fileIn;
    }
    // initialize reader
    this.pos=start;
    this.officeReader.parse();
    // read linked workbooks
    if (this.readLinkedWorkbooks==true) {
	// get current path
	Path currentPath = fSplit.getPath();
	Path parentPath = currentPath.getParent();
	// read linked workbook filenames
	List<String> linkedWorkbookList=this.officeReader.getCurrentParser().getLinkedWorkbooks();
	this.currentHFR = new HadoopFileReader(context.getConfiguration());
	if (linkedWorkbookList!=null) {
		for (String listItem: linkedWorkbookList) {
			LOG.info("Adding linked workbook \""+listItem+"\"");
			String sanitizedListItem = new Path(listItem).getName();
			// read file from hadoop file
			Path currentFile=new Path(parentPath,sanitizedListItem);
			InputStream currentIn=this.currentHFR.openFile(currentFile);
			this.officeReader.getCurrentParser().addLinkedWorkbook(listItem,currentIn,this.linkedWBCredentialMap.get(sanitizedListItem));
		}
	}
    }
} catch (FormatNotUnderstoodException fnue) {
	LOG.error(fnue); 	
	this.close();
}  catch (GeneralSecurityException gse) {
	LOG.error(gse);
	this.close();
}
}



/**
*
* Read row from Office document
*
* @return true if next more rows are available, false if not
*/
public abstract boolean nextKeyValue() throws IOException;


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
      if (decompressor != null) { // return this decompressor
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      } // return decompressor of linked workbooks
	if (this.currentHFR!=null) {
		currentHFR.close();
	}
    }
  if (this.fs!=null) {
	this.fs.close();
  }
}


}

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


import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.common.HadoopFileReader;
import org.zuinnote.hadoop.office.format.common.HadoopKeyStoreManager;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeReader;
import org.zuinnote.hadoop.office.format.common.parser.*;




/**
* This abstract reader implements some generic functionality (e.g. buffers etc.) for reading tables from various formats
*
*
*/

public abstract class AbstractSpreadSheetDocumentRecordReader<K,V> extends RecordReader<K,V> {
private static final Log LOG = LogFactory.getLog(AbstractSpreadSheetDocumentRecordReader.class.getName());

private OfficeReader officeReader=null;


private CompressionCodec codec;
private Decompressor decompressor;
private Configuration conf;
private long start;
private long end;
private Seekable filePosition;
private HadoopFileReader currentHFR;
private HadoopOfficeReadConfiguration hocr;

/**
* Creates an Abstract Record Reader for tables from various document formats
* @param conf Configuration: configuration to be parsed by HadoopOfficeConfiguration class
*
*
*/
public AbstractSpreadSheetDocumentRecordReader(Configuration conf) {
 	// parse configuration
	this.hocr=new HadoopOfficeReadConfiguration(conf);
     this.conf=conf;	
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
    codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    this.hocr.setFileName(file.getName());
    this.readKeyStore(context.getConfiguration());
    this.readTrustStore(context.getConfiguration());
    FSDataInputStream fileIn = file.getFileSystem(conf).open(file);
    // open stream
      if (isCompressedInput()) { // decompress
      	decompressor = CodecPool.getDecompressor(codec);
      	if (codec instanceof SplittableCompressionCodec) {
		LOG.debug("Reading from a compressed file \""+file+"\" with splittable compression codec");
        	final SplitCompressionInputStream cIn =((SplittableCompressionCodec)codec).createInputStream(fileIn, decompressor, start, end,SplittableCompressionCodec.READ_MODE.CONTINUOUS);
		officeReader = new OfficeReader(cIn, this.hocr);  
		start = cIn.getAdjustedStart();
       		end = cIn.getAdjustedEnd();
        	filePosition = cIn; // take pos from compressed stream
      } else {
	LOG.debug("Reading from a compressed file \""+file+"\" with non-splittable compression codec");
	officeReader = new OfficeReader(codec.createInputStream(fileIn,decompressor), this.hocr);
        filePosition = fileIn;
      }
    } else {
	LOG.debug("Reading from an uncompressed file \""+file+"\"");
      fileIn.seek(start);
	officeReader = new OfficeReader(fileIn, this.hocr);  
      filePosition = fileIn;
    }
    // initialize reader
    this.officeReader.parse();
    // read linked workbooks
    if (this.hocr.getReadLinkedWorkbooks()) {
	// get current path
	Path currentPath = fSplit.getPath();
	Path parentPath = currentPath.getParent();
	if (!"".equals(this.hocr.getLinkedWorkbookLocation())) {
		// use a custom location for linked workbooks
		parentPath = new Path(this.hocr.getLinkedWorkbookLocation());
	}
	// read linked workbook filenames
	List<String> linkedWorkbookList=this.officeReader.getCurrentParser().getLinkedWorkbooks();
	LOG.debug(linkedWorkbookList.size());
	this.currentHFR = new HadoopFileReader(context.getConfiguration());
	for (String listItem: linkedWorkbookList) {
		LOG.info("Adding linked workbook \""+listItem+"\"");
		String sanitizedListItem = new Path(listItem).getName();
		// read file from hadoop file
		Path currentFile=new Path(parentPath,sanitizedListItem);
		InputStream currentIn=this.currentHFR.openFile(currentFile);
		this.officeReader.getCurrentParser().addLinkedWorkbook(listItem,currentIn,this.hocr.getLinkedWBCredentialMap().get(sanitizedListItem));
	}
    }
} catch (FormatNotUnderstoodException fnue) {
	LOG.error(fnue); 	
	this.close();
	throw new InterruptedException();
}  
}


/**
 * Reads the keystore to obtain credentials
 * 
 * @param conf Configuration provided by the Hadoop environment
 * @throws IOException 
 * @throws FormatNotUnderstoodException
 * 
 */
private void readKeyStore(Configuration conf) throws IOException, FormatNotUnderstoodException {
	if ((this.hocr.getCryptKeystoreFile()!=null) && (!"".equals(this.hocr.getCryptKeystoreFile()))) {
		LOG.info("Using keystore to obtain credentials instead of passwords");
		HadoopKeyStoreManager hksm = new HadoopKeyStoreManager(conf);
		try {
			hksm.openKeyStore(new Path(this.hocr.getCryptKeystoreFile()), this.hocr.getCryptKeystoreType(), this.hocr.getCryptKeystorePassword());
			String pw="";
			if ((this.hocr.getCryptKeystoreAlias()!=null) && (!"".equals(this.hocr.getCryptKeystoreAlias()))) {
				pw=hksm.getPassword(this.hocr.getCryptKeystoreAlias(), this.hocr.getCryptKeystorePassword());
			} else {
				pw=hksm.getPassword(this.hocr.getFileName(), this.hocr.getCryptKeystorePassword());
			}
			this.hocr.setPassword(pw);
		} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException | UnrecoverableEntryException | InvalidKeySpecException e) {
			LOG.error("Cannopt read keystore. Exception: ",e);
			throw new FormatNotUnderstoodException("Cannot read keystore to obtain credentials to access encrypted documents "+e);
		}
		
	}
}



/***
 * Read truststore for establishing certificate chain for signature validation
 * 
 * @param conf
 * @throws IOException
 * @throws FormatNotUnderstoodException
 */
private void readTrustStore(Configuration conf) throws IOException, FormatNotUnderstoodException {
	if (((this.hocr.getSigTruststoreFile()!=null) && (!"".equals(this.hocr.getSigTruststoreFile())))) {
		LOG.info("Reading truststore to validate certificate chain for signatures");
		HadoopKeyStoreManager hksm = new HadoopKeyStoreManager(conf);
		try {
			hksm.openKeyStore(new Path(this.hocr.getSigTruststoreFile()), this.hocr.getSigTruststoreType(), this.hocr.getSigTruststorePassword());
			this.hocr.setX509CertificateChain(hksm.getAllX509Certificates());
		} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException  e) {
			LOG.error("Cannopt read truststore. Exception: ",e);
			throw new FormatNotUnderstoodException("Cannot read truststore to establish certificate chain for signature validation "+e);
		}
		
	}
}



/**
*
* Read row from Office document
*
* @return true if next more rows are available, false if not
*/
@Override
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
@Override
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
    return codec != null;
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
@Override
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
	// do not close the filesystem! will cause exceptions in Spark
 
}





}

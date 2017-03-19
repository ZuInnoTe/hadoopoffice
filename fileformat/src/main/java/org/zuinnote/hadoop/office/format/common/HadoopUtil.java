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
import java.io.DataOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.io.compress.CompressionCodec;

import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

public class HadoopUtil {

private HadoopUtil() {
}

/*
* Extracts from the configuration all properties having the same base a Map of Key/value pair. Example: base: "hadoopoffice.read.filter.metadata.", properties in JobConf "hadoopoffice.read.filter.metadata.author"="Martha Musterfrau", "hadoopoffice.read.filter.metadata.keywords"="test,document", "hadoopoffice.test"="test" will return a Map with the following elements:
* "metadata.author"="Martha Musterfrau", "metadata.keywords"="test,document" 
*
* @param conf Configuration of application
* @param base look for all keys with this base
* 
* @return Map with key/values of all keys having the specified base. Note the base is removed from the key
*
*/

public static Map<String,String> parsePropertiesFromBase(Configuration conf, String base) {
	HashMap<String,String> result = new HashMap<>();
	for (Map.Entry<String, String> currentItem : conf) {
	    String currentKey=currentItem.getKey();
            if (currentKey.startsWith(base)) {
		String strippedKey = currentKey.substring(base.length());
		result.put(strippedKey,currentItem.getValue());
	    }
        }
	return result;
}



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

public static DataOutputStream getDataOutputStream(Configuration conf,Path file, Progressable progress, boolean compressed, Class<? extends CompressionCodec> compressorClass) throws IOException {
if (!compressed) { // uncompressed
	FileSystem fs = file.getFileSystem(conf);
        return fs.create(file, progress);
} else { // compressed (note partially adapted from TextOutputFormat)
      Class<? extends CompressionCodec> codecClass = compressorClass;
      // create the named codec
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);
      // provide proper file extension
      Path compressedFile = file.suffix(codec.getDefaultExtension());
      // build the filename including the extension
      FileSystem fs = compressedFile.getFileSystem(conf);
      return new DataOutputStream(codec.createOutputStream(fs.create(compressedFile, progress)));
}
}


/*
* Parses a string in the format [filename]:[filename2]:[filename3] into a String array of filenames
*
* @param linkedWorkbookString list of filename as one String
*
* @return String Array containing the filenames as separate Strings
*
**/
public static String[] parseLinkedWorkbooks(String linkedWorkbooksString) {
	if ("".equals(linkedWorkbooksString)) {
		return new String[0];
	}
	// first split by ]:[
	String[] tempSplit = linkedWorkbooksString.split("\\]:\\[");
	// 1st case just one filename remove first [ and last ]
	if (tempSplit.length==1) {
		tempSplit[0]=tempSplit[0].substring(1,tempSplit[0].length()-1);
	} else if (tempSplit.length>1) {
	// 2nd case two or more filenames
	// remove first [
		tempSplit[0]=tempSplit[0].substring(1,tempSplit[0].length());
	// remove last ]
		tempSplit[tempSplit.length-1]=tempSplit[tempSplit.length-1].substring(0,tempSplit[tempSplit.length-1].length()-1);
	}
	return tempSplit;
}

}

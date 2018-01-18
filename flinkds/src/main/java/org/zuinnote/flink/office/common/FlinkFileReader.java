/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.flink.office.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
/**
 * @author jornfranke
 *
 */
public class FlinkFileReader {
	
	
	public InputStream openFile(Path path) throws IOException {
			FileSystem fs = FileSystem.get(path.toUri());
			return fs.open(path);
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
	HashMap<String,InputStream> result = new HashMap<>();
	if (fileNames==null) {
		return result;
	}
	for (String currentFile: fileNames) {
		Path currentPath=new Path(currentFile);
		InputStream currentInputStream = openFile(currentPath);
		result.put(currentPath.getName(), currentInputStream); 
	}
	return result;
}

/*
* Loads template as InputStreams
* 
* @param fileName filename of template (full URI/path) to load
*
* @return InputStream of the template
*
* @throws java.io.IOException in case of issues loading a file
*
*/
public InputStream loadTemplate(String fileName) throws IOException {
	Path currentPath=new Path(fileName);
	return openFile(currentPath);
}

	
	

}

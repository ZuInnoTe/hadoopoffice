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
	HashMap<String,String> result = new HashMap<String,String>();
	for (Map.Entry<String, String> currentItem : conf) {
	    String currentKey=currentItem.getKey();
            if (currentKey.startsWith(base)) {
		String strippedKey = currentKey.substring(base.length());
		result.put(strippedKey,currentItem.getValue());
	    }
        }
	return result;
}

}

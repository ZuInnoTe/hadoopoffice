/**
* Copyright 2018 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.hadoop.office.format.common.parser.msexcel.internal.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class implements a Least-Recently-Used Cache based on a LinkedHashMap
 *
 */
public class LRUCache<K,V> extends LinkedHashMap<K,V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5528483584452176992L;
	private int size;
	
	
	/*
	 * Initialize LRUCache with HashMap Default Size: 16
	 * 
	 */
	public LRUCache() {
		super(16,0.75f, true);
		this.size=16;
	}
	
	public LRUCache(int size) {
		super(size,0.75f, true);
		this.size=size;
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return size() > size;
	}

}

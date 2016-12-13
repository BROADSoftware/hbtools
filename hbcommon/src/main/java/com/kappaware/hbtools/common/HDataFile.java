/*
 * Copyright (C) 2016 BROADSoftware
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
 */
package com.kappaware.hbtools.common;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@SuppressWarnings("serial")
public class HDataFile {

	static ObjectMapper mapper;
	static {
		mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	static public String esc(String s) {
		return s.replace("\\", "\\\\").replace("\"", "\\\\x22");
	}
	
	static public class HDFamily extends HashMap<String, String> {
		private List<String> sortedColNames = null;

		HDFamily cloneCanonical() {
			HDFamily clone = new HDFamily();
			for(Map.Entry<String, String> entry: this.entrySet()) {
				clone.put(Bytes.toStringBinary(Bytes.toBytesBinary(entry.getKey())), Bytes.toStringBinary(Bytes.toBytesBinary(entry.getValue())));
			}
			return clone;
		}
		
		public List<String> getSortedColNames() {
			if(this.sortedColNames == null) {
				this.sortedColNames = new Vector<String>(this.keySet());
				Collections.sort(this.sortedColNames, new Comparator<String>(){
					@Override
					public int compare(String o1, String o2) {
						return Bytes.compareTo(Bytes.toBytesBinary(o1), Bytes.toBytesBinary(o2));
					}
				});
			}
			return this.sortedColNames;
		}
		
		public String toJson() {
			StringBuffer sb = new StringBuffer();
			String sep = " ";
			sb.append("{");
			for(String colName : this.getSortedColNames()) {
				sb.append(sep + "\"");
				sb.append(esc(colName));
				sb.append("\": \"" );
				sb.append(esc(this.get(colName)));
				sb.append('"');
				sep = ", ";
			}
			sb.append(" }");
			return sb.toString();
		}
	}

	static public class HDRow extends HashMap<String, HDFamily> {
		private List<String> sortedColFamNames = null;

		HDRow cloneCanonical() {
			HDRow clone = new HDRow();
			for(Map.Entry<String, HDFamily> entry: this.entrySet()) {
				clone.put(Bytes.toStringBinary(Bytes.toBytesBinary(entry.getKey())), entry.getValue().cloneCanonical());
			}
			return clone;
		}

		
		public List<String> getSortedColFamNames() {
			if(this.sortedColFamNames == null) {
				this.sortedColFamNames = new Vector<String>(this.keySet());
				Collections.sort(this.sortedColFamNames, new Comparator<String>(){
					@Override
					public int compare(String o1, String o2) {
						return Bytes.compareTo(Bytes.toBytesBinary(o1), Bytes.toBytesBinary(o2));
					}
				});
			}
			return this.sortedColFamNames;
		}

		public String toJson() {
			StringBuffer sb = new StringBuffer();
			String sep = " ";
			sb.append("{");
			for(String colFamName : this.getSortedColFamNames()) {
				sb.append(sep + "\"");
				sb.append(esc(colFamName));
				sb.append("\": " );
				sb.append(this.get(colFamName).toJson());
				sep = ", ";
			}
			sb.append(" }");
			return sb.toString();
		}

		public String getValue(String colFamilyName, String colName) {
			HDFamily colFamily = this.get(colFamilyName);
			if(colFamily != null) { 
				return colFamily.get(colName);
			} else {
				return null;
			}
		}

		public void removeValue(String colFamilyName, String colName) {
			HDFamily colFamily = this.get(colFamilyName);
			if(colFamily != null) {
				colFamily.remove(colName);
			} 
		}
	}

	static public class HDTable extends HashMap<String, HDRow> {
		private List<String> sortedRowNames = null;

		/**
		 * Issue is the same binary string can have two representation. For example, "\x2EAAA" and ".AAA" represent the same byte[]
		 * This will lead a lot of missbehavior.
		 * To prevent this. we provide the cloneCanonical() function, which will adjust to ".AAA", the standard way for HBase 
		 * @return
		 */
		public HDTable cloneCanonical() {
			HDTable clone = new HDTable();
			for(Map.Entry<String, HDRow> entry: this.entrySet()) {
				clone.put(Bytes.toStringBinary(Bytes.toBytesBinary(entry.getKey())), entry.getValue().cloneCanonical());
			}
			return clone;
		}
		
		
		public void addRow(String rowKey, HDRow rowValues) {
			this.put(rowKey, rowValues);
		}

		public String toJsonString() throws JsonProcessingException {
			return mapper.writeValueAsString(this);
		}

		static public HDTable fromJson(String json) throws JsonParseException, JsonMappingException, IOException {
			return mapper.readValue(json, HDTable.class);
		}

		static public HDTable fromFile(File file) throws JsonParseException, JsonMappingException, IOException {
			return mapper.readValue(file, HDTable.class);
		}
		
		public List<String> getSortedRowkeys() {
			if(this.sortedRowNames == null) {
				this.sortedRowNames = new Vector<String>(this.keySet());
				Collections.sort(this.sortedRowNames, new Comparator<String>(){
					@Override
					public int compare(String o1, String o2) {
						return Bytes.compareTo(Bytes.toBytesBinary(o1), Bytes.toBytesBinary(o2));
					}
				});
			}
			return this.sortedRowNames;
		}

		String toJson() {
			StringBuffer sb = new StringBuffer();
			String sep = " ";
			sb.append("{\n");
			for(String rowkey : this.getSortedRowkeys()) {
				sb.append("   " + sep + "\"");
				sb.append(esc(rowkey));
				sb.append("\": " );
				sb.append(this.get(rowkey).toJson());
				sep = ",";
				sb.append("\n");
			}
			sb.append("}\n");
			return sb.toString();
		}
		
	}
}

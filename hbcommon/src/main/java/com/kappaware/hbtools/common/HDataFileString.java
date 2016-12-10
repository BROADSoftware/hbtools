package com.kappaware.hbtools.common;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@SuppressWarnings("serial")
public class HDataFileString {

	static ObjectMapper mapper;
	static {
		mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	static public class ColFamString extends HashMap<String, String> {
		private List<String> sortedColNames = null;

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
				sb.append(colName.replace("\\", "\\\\"));
				sb.append("\": \"" );
				sb.append(this.get(colName).replace("\\", "\\\\"));
				sb.append('"');
				sep = ", ";
			}
			sb.append("}");
			return sb.toString();
		}
	}

	static public class RowString extends HashMap<String, ColFamString> {
		private List<String> sortedColFamNames = null;

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
				sb.append(colFamName.replace("\\", "\\\\"));
				sb.append("\": " );
				sb.append(this.get(colFamName).toJson());
				sep = ", ";
			}
			sb.append("}");
			return sb.toString();
		}
	}

	static public class TableString extends HashMap<String, RowString> {
		private List<String> sortedRowNames = null;

		public void addRow(String rowKey, RowString rowValues) {
			this.put(rowKey, rowValues);
		}

		public String toJsonString() throws JsonProcessingException {
			return mapper.writeValueAsString(this);
		}

		static public TableString fromJson(String json) throws JsonParseException, JsonMappingException, IOException {
			return mapper.readValue(json, TableString.class);
		}

		static public TableString fromFile(File file) throws JsonParseException, JsonMappingException, IOException {
			return mapper.readValue(file, TableString.class);
		}
		
		public List<String> getSortedRowNames() {
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
			for(String rowName : this.getSortedRowNames()) {
				sb.append("   " + sep + "\"");
				sb.append(rowName.replace("\\", "\\\\"));
				sb.append("\": " );
				sb.append(this.get(rowName).toJson());
				sep = ",";
				sb.append("\n");
			}
			sb.append("}\n");
			return sb.toString();
		}
		

		public String toJsonStringSpecxx() {
			StringBuffer sb = new StringBuffer();
			sb.append("{\n");
			List<String> keys = new Vector<String>(this.keySet());
			Collections.sort(keys);
			String sepKey = " ";
			for (String k : keys) {
				sb.append("   " + sepKey);
				sepKey = ",";
				sb.append('"');
				sb.append(k.replace("\\", "\\\\"));
				sb.append("\": {");
				RowString row = this.get(k);
				List<String> cfs = new Vector<String>(row.keySet());
				Collections.sort(cfs);
				String sepCf = " ";
				for (String cf : cfs) {
					sb.append(sepCf);
					sepCf = ", ";
					sb.append('"');
					sb.append(cf.replace("\\", "\\\\"));
					sb.append("\": {");
					ColFamString colFam = row.get(cf);
					List<String> cols = new Vector<String>(colFam.keySet());
					Collections.sort(cols);
					String sepCol = " ";
					for (String col : cols) {
						sb.append(sepCol);
						sepCol = ", ";
						sb.append('"');
						sb.append(col.replace("\\", "\\\\"));
						sb.append("\": \"");
						sb.append(colFam.get(col).replace("\\", "\\\\"));
						sb.append('"');
					}
					sb.append("}");
				}
				sb.append("}\n");
			}
			sb.append("}\n");
			return sb.toString();
		}
	}
}

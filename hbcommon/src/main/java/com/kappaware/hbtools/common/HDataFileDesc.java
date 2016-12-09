package com.kappaware.hbtools.common;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@SuppressWarnings("serial")
public class HDataFileDesc extends HashMap<String, Map<String, Map<String, String>>> {

	static ObjectMapper mapper;
	static {
		mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
	}

	public void addRow(String rowKey, Map<String, Map<String, String>> rowValues) {
		this.put(rowKey, rowValues);
	}

	public String toJsonString() throws JsonProcessingException {
		return mapper.writeValueAsString(this);
	}

	static public HDataFileDesc fromJson(String json) throws JsonParseException, JsonMappingException, IOException {
		return mapper.readValue(json, HDataFileDesc.class);
	}

	static public HDataFileDesc fromFile(File file) throws JsonParseException, JsonMappingException, IOException {
		return mapper.readValue(file, HDataFileDesc.class);
	}

	
	public String toJsonStringSpec() {
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
			Map<String, Map<String, String>> cfMap = this.get(k);
			List<String> cfs = new Vector<String>(cfMap.keySet());
			Collections.sort(cfs);
			String sepCf = " ";
			for (String cf : cfs) {
				sb.append(sepCf);
				sepCf = ", ";
				sb.append('"');
				sb.append(cf.replace("\\", "\\\\"));
				sb.append("\": {");
				Map<String, String> colMap = cfMap.get(cf);
				List<String> cols = new Vector<String>(colMap.keySet());
				Collections.sort(cols);
				String sepCol = " ";
				for(String col : cols) {
					sb.append(sepCol);
					sepCol = ", ";
					sb.append('"');
					sb.append(col.replace("\\", "\\\\"));
					sb.append("\": \"");
					sb.append(colMap.get(col).replace("\\", "\\\\"));
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

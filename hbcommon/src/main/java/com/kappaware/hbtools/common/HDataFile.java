package com.kappaware.hbtools.common;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@SuppressWarnings("serial")
public class HDataFile  {
	static Logger log = LoggerFactory.getLogger(HDataFile.class);

	static public class CfBinary extends HashMap<byte[], byte[]> {
	}
	
	static public class RowBinary extends HashMap<byte[], CfBinary> {
	}
	
	static public class TableBinary extends HashMap<byte[], RowBinary> {
		public TableBinary(HDataFileDesc d1) {
			for (String rk1 : d1.keySet()) {
				Map<String, Map<String, String>> row1 = d1.get(rk1);
				byte[] rk2 = Bytes.toBytesBinary(rk1);
				RowBinary row2 = new RowBinary();
				for(String cf1 : row1.keySet()) {
					Map<String, String> cfValue1 = row1.get(cf1);
					byte[] cf2 = Bytes.toBytesBinary(cf1);
					CfBinary cfValue2 = new CfBinary();
					for(String col1 : cfValue1.keySet()) {
						String value1 = cfValue1.get(col1);
						byte[] value2 = Bytes.toBytesBinary(value1);
						byte[] col2 = Bytes.toBytesBinary(col1);
						cfValue2.put(col2, value2);
					}
					row2.put(cf2, cfValue2);
				}
				this.put(rk2, row2);
			}
		}

		
		private List<byte[]> sortedRowkey = null;
		
		public List<byte[]> getSortedRowkey() {
			if(this.sortedRowkey == null) {
				this.sortedRowkey = new Vector<byte[]>(this.keySet());
				Collections.sort(this.sortedRowkey, new Comparator<byte[]>(){
					@Override
					public int compare(byte[] o1, byte[] o2) {
						return Bytes.compareTo(o1, o2);
					}
				});
				if(log.isDebugEnabled()) {
					for(byte[] rk: this.sortedRowkey) {
						log.debug("rowkey:" + Bytes.toStringBinary(rk));
					}
				}
			}
			return this.sortedRowkey;
		}
		
	}
	
}

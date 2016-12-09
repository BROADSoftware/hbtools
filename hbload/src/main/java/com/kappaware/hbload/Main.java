package com.kappaware.hbload;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.HDataFile;
import com.kappaware.hbtools.common.HDataFileDesc;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	
	static public void main2(String[] argv) throws ConfigurationException, JsonParseException, JsonMappingException, IOException {
		log.info("hbload start");
		Parameters parameters = new Parameters(argv);
		File file = new File(parameters.getInputFile());
		if (!file.canRead()) {
			throw new ConfigurationException(String.format("Unable to open '%s' for reading", file.getAbsolutePath()));
		}
		HDataFileDesc dataString = HDataFileDesc.fromFile(file);
		HDataFile.TableBinary data = new HDataFile.TableBinary(dataString);
		// The following will remove the message: 2014-06-14 01:38:59.359 java[993:1903] Unable to load realm info from SCDynamicStore
		// Equivalent to HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.conf=/dev/null"
		// Of course, should be configured properly in case of use of Kerberos
		//System.setProperty("java.security.krb5.conf", "/dev/null");

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", parameters.getZookeeper());
		config.set("zookeeper.znode.parent", parameters.getZnodeParent());
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tableName = TableName.valueOf(parameters.getNamespace(), parameters.getTable());
		Table table = connection.getTable(tableName);
		BufferedMutator mutator = connection.getBufferedMutator(tableName);
		try {
			if(parameters.isDelRow()) {
				// In such case, we must scan all table to remove undefined row.
				
			} else {
				// In such case, we will only pick up row if interest
				for(byte[] rowKey : data.getSortedRowkey()) {
					Result result = table.get(new Get(rowKey));
					if(result.size() == 0) {
						if(parameters.isAddRow()) {
							log.debug(String.format("Will add a full row for rowkey '%s'", Bytes.toStringBinary(rowKey)));
							Put put = new Put(rowKey);
							
							
							
							
						} else {
							log.debug(String.format("rowkey %s does not exist, by addRow is not set",  Bytes.toStringBinary(rowKey)));
						}
						
					} else {
						
					}
				}
				
			}
			
			
			
			
		} finally {
			table.close();
			connection.close();
		}
	}

}

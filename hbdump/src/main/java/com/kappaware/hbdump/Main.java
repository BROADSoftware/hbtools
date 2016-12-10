package com.kappaware.hbdump;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.HDataFileBinary.ColFamBinary;
import com.kappaware.hbtools.common.HDataFileBinary.RowBinary;
import com.kappaware.hbtools.common.HDataFileString;
import com.kappaware.hbtools.common.HDataFileString.ColFamString;
import com.kappaware.hbtools.common.HDataFileString.RowString;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) {
		log.info("hbdump start");
		log.debug("hbdump DEBUG enabled");
		try {
			main2(argv);
		} catch (ConfigurationException | IOException e) {
			log.error(e.getMessage());
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		}
	}

	static public void main2(String[] argv) throws ConfigurationException, IOException {
		Parameters parameters = new Parameters(argv);
		// The following will remove the message: 2014-06-14 01:38:59.359 java[993:1903] Unable to load realm info from SCDynamicStore
		// Equivalent to HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.conf=/dev/null"
		// Of course, should be configured properly in case of use of Kerberos
		//System.setProperty("java.security.krb5.conf", "/dev/null");

		PrintStream out = System.out;
		if(parameters.getOutputFile() != null) {
			File file = new File(parameters.getOutputFile());
			try {
				out = new PrintStream(file);
			} catch (FileNotFoundException e) {
				throw new ConfigurationException(String.format("Unable to open '%s' for writing", file.getAbsolutePath()));
			}
		}
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", parameters.getZookeeper());
		config.set("zookeeper.znode.parent", parameters.getZnodeParent());
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tableName = TableName.valueOf(parameters.getNamespace(), parameters.getTable());
		Table table = connection.getTable(tableName);
		try {
			out.print("{\n");
			String sep1 = " ";
			Scan scan1 = new Scan();
			ResultScanner scanner1 = table.getScanner(scan1);
			for (Result result : scanner1) {
				//System.out.println(result);
				List<Cell> cells = result.listCells();
				RowString row = new RowString();
				for(Cell cell : cells) {
					String colFamName = toStr(CellUtil.cloneFamily(cell));
					ColFamString colFam = row.get(colFamName);
					if(colFam == null) {
						colFam = new ColFamString();
						row.put(colFamName, colFam);
					}
					colFam.put(toStr(CellUtil.cloneQualifier(cell)), toStr(CellUtil.cloneValue(cell)));
				}
				String js = toJson(toStr(result.getRow()), row);
				out.print("   " + sep1 + js);
				sep1 = ",";
			}
			out.print("}\n");
			scanner1.close();
		} finally {
			table.close();
			connection.close();
		}
	}
	
	private static String toStr(byte[] ba) {
		return Bytes.toStringBinary(ba).replace("\\", "\\\\");
	}

	private static String toJson(String rowKey, RowString row) {
		StringBuffer sb = new StringBuffer();
		sb.append('"');
		sb.append(rowKey);
		sb.append("\": ");
		sb.append(row.toJson());
		sb.append("\n");
		return sb.toString();
	}
}

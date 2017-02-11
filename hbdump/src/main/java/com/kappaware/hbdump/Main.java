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
package com.kappaware.hbdump;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import com.kappaware.hbtools.common.HDataFile;
import com.kappaware.hbtools.common.HDataFile.HDFamily;
import com.kappaware.hbtools.common.HDataFile.HDRow;
import com.kappaware.hbtools.common.ParserHelpException;
import com.kappaware.hbtools.common.Utils;

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
		} catch (ParserHelpException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
	}

	static public void main2(String[] argv) throws ConfigurationException, IOException, ParserHelpException {
		Parameters parameters = new Parameters(argv);

		PrintStream out = System.out;
		if(parameters.getOutputFile() != null) {
			File file = new File(parameters.getOutputFile());
			try {
				out = new PrintStream(file);
			} catch (FileNotFoundException e) {
				throw new ConfigurationException(String.format("Unable to open '%s' for writing", file.getAbsolutePath()));
			}
		}
		Configuration config = Utils.buildHBaseConfiguration(parameters); 
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
				HDRow row = new HDRow();
				for(Cell cell : cells) {
					String colFamName = toStr(CellUtil.cloneFamily(cell));
					HDFamily colFam = row.get(colFamName);
					if(colFam == null) {
						colFam = new HDFamily();
						row.put(colFamName, colFam);
					}
					colFam.put(toStr(CellUtil.cloneQualifier(cell)), toStr(CellUtil.cloneValue(cell)));
				}
				String js = toJson(result.getRow(), row);
				out.print("   " + sep1 + js);
				sep1 = ",";
			}
			out.print("}\n");
			out.flush();
			if(out != System.out) {
				out.close();
			}
			scanner1.close();
		} finally {
			table.close();
			connection.close();
		}
	}
	
	private static String toStr(byte[] ba) {
		return Bytes.toStringBinary(ba);
	}

	private static String toJson(byte[] rowKey, HDRow row) {
		StringBuffer sb = new StringBuffer();
		sb.append('"');
		sb.append(HDataFile.esc(toStr(rowKey)));
		sb.append("\": ");
		sb.append(row.toJson());
		sb.append("\n");
		return sb.toString();
	}
}

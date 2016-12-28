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
package com.kappaware.hbload;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hbtools.common.HDataFile.HDFamily;
import com.kappaware.hbtools.common.HDataFile.HDRow;
import com.kappaware.hbtools.common.HDataFile.HDTable;

public class Engine {
	static Logger log = LoggerFactory.getLogger(Engine.class);

	private Parameters parameters;
	private HDTable data;
	private Configuration config;
	private TableName tableName;

	private Connection connection;
	private Table table;
	private BufferedMutator mutator;

	Engine(Parameters parameters, HDTable data) {
		this.parameters = parameters;
		this.data = data;
		this.config = HBaseConfiguration.create();
		this.config.set("hbase.zookeeper.quorum", parameters.getZookeeper());
		this.config.set("zookeeper.znode.parent", parameters.getZnodeParent());
		this.tableName = TableName.valueOf(parameters.getNamespace(), parameters.getTable());
	}

	void run() throws IOException {
		connection = ConnectionFactory.createConnection(config);
		table = connection.getTable(tableName);
		mutator = connection.getBufferedMutator(tableName);
		int mutationCount = 0;
		try {
			// First, we will handle row presents in the provided dataset
			for (String rowKey : data.getSortedRowkeys()) {
				Result result = table.get(new Get(Bytes.toBytesBinary(rowKey)));
				if (result.size() == 0) {
					if (parameters.isAddRow()) {
						log.debug(String.format("Will add a new row for rowkey '%s'", rowKey));
						this.addRow(rowKey);
						mutationCount++;
					} else {
						log.debug(String.format("Will NOT add row for rowkey '%s' as --dontAddRow is set", rowKey));
					}
				} else {
					//log.debug(String.format("Will adjust row '%s'", rowKey));
					mutationCount += this.adjustRow(result);
				}
			}
			// Now, will handle row removal, if any
			if (parameters.isDelRows()) {
				// In such case, we must scan all table to remove undefined row.
				// We store row to delete in memory, as I am afraid of deletion side effect on the scanner.
				List<byte[]> rowkeyToDelete = new Vector<byte[]>();
				Scan scan = new Scan();
				ResultScanner scanner = table.getScanner(scan);
				for (Result result : scanner) {
					byte[] row = result.getRow();
					if(!data.containsKey(Bytes.toStringBinary(row))) {
						log.debug(String.format("Will delete row for rowkey '%s'", Bytes.toStringBinary(row)));
						rowkeyToDelete.add(row);
					}
				}
				scanner.close();
				// And now, effective delete
				for(byte[] row : rowkeyToDelete) {
					Delete delete = new Delete(row);
					mutator.mutate(delete);
					mutationCount++;
				}
			} else {
				log.debug(String.format("No check will be performed for row removal, as --delRows is not set."));
			}
			mutator.flush();
		} finally {
			mutator.close();
			table.close();
			connection.close();
		}
		String m = String.format("hbload: %d modification(s)", mutationCount);
		System.out.println(m);
		log.info(m);
	}

	/**
	 * 
	 * @param result
	 * @return The number of mutations
	 * @throws IOException
	 */
	private int adjustRow(Result result) throws IOException {
		List<Cell> cells = result.listCells();
		PutWrapper put = new PutWrapper(result.getRow());
		DeleteWrapper del = new DeleteWrapper(result.getRow());
		String rowkey = Bytes.toStringBinary(result.getRow());
		//log.debug(String.format("Will loookup '%s'", rowkey));
		HDRow row = data.get(rowkey);
		// First, handle Cell present in HBase table
		for (Cell cell : cells) {
			CellWrapper cellw = new CellWrapper(cell);
			String newValue = row.getValue(cellw.getFamillyString(), cellw.getQualifierString());
			if (newValue == null) {
				if (parameters.isDelValues()) {
					log.debug(String.format("Will delete cell '%s'", cellw.toString()));
					del.addColumn(cellw.getFamillyByte(), cellw.getQualifierByte());
				} else {
					log.debug(String.format("Will NOT delete cell '%s' as --delValues is not set", cellw.toString()));
				}
			} else {
				if (!cellw.getValueString().equals(newValue)) {
					if (this.parameters.isUpdValues()) {
						log.debug(String.format("Will update cell '%s' with '%s'", cellw.toString(), newValue));
						put.add(cellw.getFamillyByte(), cellw.getQualifierByte(), newValue);
					} else {
						log.debug(String.format("Will NOT update cell '%s' with '%s' as --updValues is not set ", cellw.toString(), newValue));
					}
				}
				// We remove from the dataset to mark as handled
				row.removeValue(cellw.getFamillyString(), cellw.getQualifierString());
			}
		}
		// Now, we loop in remaining cell (Present in dataset, not in table)
		for (String colFamilyName : row.keySet()) {
			HDFamily colFamilly = row.get(colFamilyName);
			for (String colName : colFamilly.keySet()) {
				if (this.parameters.isAddValue()) {
					log.debug(String.format("Will add cell '%s:%s:%s:%s'", rowkey, colFamilyName, colName, colFamilly.get(colName)));
					put.add(colFamilyName, colName, colFamilly.get(colName));
				} else {
					log.debug(String.format("Will NOT add cell '%s:%s:%s:%s'as  --dontAddValue is set", rowkey, colFamilyName, colName, colFamilly.get(colName)));

				}
			}
		}
		put.mutate(mutator);
		del.mutate(mutator);
		return put.getMutationCount() + del.getMutationCount();
	}

	private void addRow(String rowKey) throws IOException {
		PutWrapper put = new PutWrapper(rowKey);
		HDRow row = data.get(rowKey);
		for (String colFamName : row.keySet()) {
			HDFamily colFam = row.get(colFamName);
			for (String colName : colFam.keySet()) {
				put.add(colFamName, colName, colFam.get(colName));
			}
		}
		put.mutate(mutator);
	}
}

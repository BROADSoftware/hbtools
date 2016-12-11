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

import com.kappaware.hbtools.common.HDataFileString.ColFamString;
import com.kappaware.hbtools.common.HDataFileString.RowString;
import com.kappaware.hbtools.common.HDataFileString.TableString;

public class Engine {
	static Logger log = LoggerFactory.getLogger(Engine.class);

	private Parameters parameters;
	private TableString data;
	private Configuration config;
	private TableName tableName;

	private Connection connection;
	private Table table;
	private BufferedMutator mutator;

	Engine(Parameters parameters, TableString data) {
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
		try {
			// First, we will handle row presents in the provided dataset
			for (String rowKey : data.getSortedRowkeys()) {
				Result result = table.get(new Get(Bytes.toBytesBinary(rowKey)));
				if (result.size() == 0) {
					if (parameters.isAddRow()) {
						log.debug(String.format("Will add a full row for rowkey '%s'", rowKey));
						this.addRow(rowKey);
					} else {
						log.debug(String.format("rowkey %s does not exist, but dontAddRow is set", rowKey));
					}
				} else {
					this.adjustRow(result);
				}
			}
			// Now, will handle row removal, if any
			if (parameters.isDelRow()) {
				// In such case, we must scan all table to remove undefined row.
				// We store row to delete in memory, as I am afraid of deletion side effect on the scanner.
				List<byte[]> rowkeyToDelete = new Vector<byte[]>();
				Scan scan = new Scan();
				ResultScanner scanner = table.getScanner(scan);
				for (Result result : scanner) {
					byte[] row = result.getRow();
					if(!data.containsKey(Bytes.toStringBinary(row))) {
						rowkeyToDelete.add(row);
					}
				}
				scanner.close();
				// And now, effective delete
				for(byte[] row : rowkeyToDelete) {
					Delete delete = new Delete(row);
					mutator.mutate(delete);
				}
			} 
			mutator.flush();
		} finally {
			mutator.close();
			table.close();
			connection.close();
		}
	}

	private void adjustRow(Result result) throws IOException {
		List<Cell> cells = result.listCells();
		PutWrapper put = new PutWrapper(result.getRow());
		DeleteWrapper del = new DeleteWrapper(result.getRow());
		String rowkey = Bytes.toStringBinary(result.getRow());
		RowString row = data.get(rowkey);
		// First, handle Cell present in HBase table
		for (Cell cell : cells) {
			CellWrapper cellw = new CellWrapper(cell);
			String newValue = row.getValue(cellw.getFamillyString(), cellw.getQualifierString());
			if (newValue == null) {
				if (parameters.isDelValue()) {
					log.debug(String.format("Will delete cell '%s'", cellw.toString()));
					del.addColumn(cellw.getFamillyByte(), cellw.getQualifierByte());
				} else {
					log.debug(String.format("Cell '%s' not in provided dataset, but delValue is not set", cellw.toString()));
				}
			} else {
				if (!cellw.getValueString().equals(newValue)) {
					if (this.parameters.isUpdValue()) {
						log.debug(String.format("Will update cell '%s' with '%s'", cellw.toString(), newValue));
						put.add(cellw.getFamillyByte(), cellw.getQualifierByte(), newValue);
					} else {
						log.debug(String.format("Will NOT update cell '%s' with '%s'. updValue is not set ", cellw.toString(), newValue));
					}
				}
				// We remove from the dataset to mark as handled
				row.removeValue(cellw.getFamillyString(), cellw.getQualifierString());
			}
		}
		// Now, we loop in remaining cell (Present in dataset, not in table)
		for (String colFamilyName : row.keySet()) {
			ColFamString colFamilly = row.get(colFamilyName);
			for (String colName : colFamilly.keySet()) {
				if (this.parameters.isAddValue()) {
					log.debug(String.format("Will add cell '%s:%s:%s:%s'", rowkey, colFamilly, colName, colFamilly.get(colName)));
					put.add(colFamilyName, colName, colFamilly.get(colName));
				} else {
					log.debug(String.format("Will NOT add cell '%s:%s:%s:%s'. dontAddValue is set", rowkey, colFamilly, colName, colFamilly.get(colName)));

				}
			}
		}
		put.mutate(mutator);
		del.mutate(mutator);
	}

	private void addRow(String rowKey) throws IOException {
		PutWrapper put = new PutWrapper(rowKey);
		RowString row = data.get(rowKey);
		for (String colFamName : row.keySet()) {
			ColFamString colFam = row.get(colFamName);
			for (String colName : colFam.keySet()) {
				put.add(colFamName, colName, colFam.get(colName));
			}
		}
		put.mutate(mutator);
	}
}

package com.kappaware.hbload;

import java.io.IOException;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;

public class DeleteWrapper {
	private Delete delete;
	private int mutation = 0;
	
	DeleteWrapper(byte[] rowkey) {
		this.delete = new Delete(rowkey);
	}

	public void addColumn(byte[] famillyByte, byte[] qualifierByte) {
		this.mutation++;
		this.delete.addColumns(famillyByte, qualifierByte);
	}

	public void mutate(BufferedMutator mutator) throws IOException {
		if(this.mutation > 0) {
			mutator.mutate(this.delete);
		}
	}

}

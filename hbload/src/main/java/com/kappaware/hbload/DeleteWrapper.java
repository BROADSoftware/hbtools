package com.kappaware.hbload;

import java.io.IOException;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;

public class DeleteWrapper {
	private Delete delete;
	private int mutationCount = 0;
	
	DeleteWrapper(byte[] rowkey) {
		this.delete = new Delete(rowkey);
	}

	public void addColumn(byte[] famillyByte, byte[] qualifierByte) {
		this.mutationCount++;
		this.delete.addColumns(famillyByte, qualifierByte);
	}

	public void mutate(BufferedMutator mutator) throws IOException {
		if(this.mutationCount > 0) {
			mutator.mutate(this.delete);
		}
	}

	public int getMutationCount() {
		return this.mutationCount;
	}

}

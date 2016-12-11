package com.kappaware.hbload;

import java.io.IOException;

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutWrapper {
	private Put put;
	private int mutation = 0;
	
	PutWrapper(byte[] rowkey) {
		this.put = new Put(rowkey);
	}

	public PutWrapper(String rowkey) {
		this.put = new Put(Bytes.toBytesBinary(rowkey));
	}

	public void add(byte[] famillyByte, byte[] qualifierByte, String value) {
		this.mutation++;
		put.addColumn(famillyByte, qualifierByte, Bytes.toBytesBinary(value));
	}

	public void add(String colFamilyName, String colName, String value) {
		this.mutation++;
		put.addColumn(Bytes.toBytesBinary(colFamilyName), Bytes.toBytesBinary(colName), Bytes.toBytesBinary(value));
		
	}

	public void mutate(BufferedMutator mutator) throws IOException {
		if(this.mutation > 0) {
			mutator.mutate(this.put);
		}
	}



}

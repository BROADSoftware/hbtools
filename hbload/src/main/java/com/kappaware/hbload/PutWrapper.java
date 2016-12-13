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

import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutWrapper {
	private Put put;
	private int mutationCount = 0;
	
	PutWrapper(byte[] rowkey) {
		this.put = new Put(rowkey);
	}

	public PutWrapper(String rowkey) {
		this.put = new Put(Bytes.toBytesBinary(rowkey));
	}

	public void add(byte[] famillyByte, byte[] qualifierByte, String value) {
		this.mutationCount++;
		put.addColumn(famillyByte, qualifierByte, Bytes.toBytesBinary(value));
	}

	public void add(String colFamilyName, String colName, String value) {
		this.mutationCount++;
		put.addColumn(Bytes.toBytesBinary(colFamilyName), Bytes.toBytesBinary(colName), Bytes.toBytesBinary(value));
		
	}

	public void mutate(BufferedMutator mutator) throws IOException {
		if(this.mutationCount > 0) {
			mutator.mutate(this.put);
		}
	}

	public int getMutationCount() {
		return this.mutationCount;
	}



}

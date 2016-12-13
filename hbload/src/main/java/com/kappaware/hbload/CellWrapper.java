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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class CellWrapper {
	private Cell cell;
	private byte[] rowkeyByte = null;
	private String rowkeyString = null;
	private byte[] familyByte = null;
	private String familyString = null;
	private byte[] qualifierByte = null;
	private String qualifierString = null;
	private byte[] valueByte = null;
	private String valueString = null;
	
	CellWrapper(Cell cell) {
		this.cell = cell;
	}
	
	
	public byte[] getRowkeyByte() {
		if(this.rowkeyByte == null) {
			this.rowkeyByte = CellUtil.cloneRow(cell);
		}
		return this.rowkeyByte;
	}
	
	public String getRowkeyString() {
		if(this.rowkeyString == null) {
			this.rowkeyString = Bytes.toStringBinary(this.getRowkeyByte());
		}
		return this.rowkeyString;
	}
	
	public byte[] getFamillyByte() {
		if(this.familyByte == null) {
			this.familyByte = CellUtil.cloneFamily(cell);
		}
		return this.familyByte;
	}
	
	public String getFamillyString() {
		if(this.familyString == null) {
			this.familyString = Bytes.toStringBinary(this.getFamillyByte());
		}
		return this.familyString;
	}
	
	public byte[] getQualifierByte() {
		if(this.qualifierByte == null) {
			this.qualifierByte = CellUtil.cloneQualifier(cell);
		}
		return this.qualifierByte;
	}

	public String getQualifierString() {
		if(this.qualifierString == null) {
			this.qualifierString = Bytes.toStringBinary(this.getQualifierByte());
		}
		return this.qualifierString;
	}
	
	public byte[] getValueByte() {
		if(this.valueByte == null) {
			this.valueByte = CellUtil.cloneValue(cell);
		}
		return this.valueByte;
	}
	
	public String getValueString() {
		if(this.valueString == null) {
			this.valueString = Bytes.toStringBinary(this.getValueByte());
		}
		return this.valueString;
	}
	
	@Override
	public String toString() {
		return String.format("%s:%s:%s:%s", this.getRowkeyString(),  this.getFamillyString(), this.getQualifierString(), this.getValueString());
	}
	
}

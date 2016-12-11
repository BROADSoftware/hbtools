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

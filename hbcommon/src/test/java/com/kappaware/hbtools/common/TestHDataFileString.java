package com.kappaware.hbtools.common;


import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.kappaware.hbtools.common.HDataFile.HDFamily;
import com.kappaware.hbtools.common.HDataFile.HDRow;
import com.kappaware.hbtools.common.HDataFile.HDTable;


public class TestHDataFileString {

	public HDTable buildTestSet() {
		HDFamily rowAcf1 = new HDFamily();
		rowAcf1.put("rowAcf1Col_a", "rowAcf1Value_a");
		rowAcf1.put("rowAcf1Col_b", "rowAcf1Value_b");
		rowAcf1.put("rowAcf1Col_c", "rowAcf1Value_c");
		//rowAcf1.put("rowAcf1Col\\x0a_d", "rowAcf1Value\\x09_d");
		HDFamily rowAcf2 = new HDFamily();
		rowAcf2.put("rowAcf2Col_a", "rowAcf2Value_a");
		rowAcf2.put("rowAcf2Col_b", "rowAcf2Value_b");
		rowAcf2.put("rowAcf2Col_c", "rowAcf2Value_c");

		HDRow rowA = new HDRow();
		rowA.put("cf1", rowAcf1);
		rowA.put("cf2", rowAcf2);
		
		
		HDFamily rowBcf1 = new HDFamily();
		rowBcf1.put("rowBcf1Col_a", "rowBcf1Value_a");
		rowBcf1.put("rowBcf1Col_b", "rowBcf1Value_b");
		HDFamily rowBcf2 = new HDFamily();
		rowBcf2.put("rowBcf2Col_a", "rowBcf2Value_a");
		rowBcf2.put("rowBcf2Col_b", "rowBcf2Value_b");
		rowBcf2.put("rowBcf2Col_c", "rowBcf2Value_c");

		HDRow rowB = new HDRow();
		rowB.put("cf1", rowBcf1);
		rowB.put("cf2", rowBcf2);

		
		HDFamily rowCcf1 = new HDFamily();
		rowCcf1.put("rowCcf1Col_a", "rowCcf1Value_a");
		rowCcf1.put("rowCcf1Col_b", "rowCcf1Value_b");
		rowCcf1.put("rowCcf1Col_c", "rowCcf1Value_c");
		
		HDRow rowC = new HDRow();
		rowC.put("cf1", rowCcf1);

		HDTable hdata = new HDTable();
		hdata.addRow("rowA", rowA);
		hdata.addRow("rowB", rowB);
		hdata.addRow("rowC", rowC);
		
		return hdata;
		
	}

	@Test
	public void test1() throws IOException  {
		HDTable d = this.buildTestSet();
		String s1 = d.toJsonString();
		HDTable d2 = HDTable.fromJson(s1);
		String s2 = d2.toJsonString();
		Assert.assertEquals(s1, s2);
		/*
		System.out.println(s1);
		System.out.println("-------------------------------");
		System.out.println(s2);
		*/
	}


	@Test
	public void test2() throws IOException  {
		HDTable d1 = this.buildTestSet();
		HDTable d2 = HDTable.fromJson(d1.toJson());

		Assert.assertEquals(d1.toJson(), d2.toJson());
		/*
		System.out.println(d2.toJsonString());
		System.out.println("-------------------------------");
		System.out.println(d2.toJson());
		*/
	}

	public HDTable buildTestSet2() {
		HDFamily rowAcf1 = new HDFamily();
		rowAcf1.put("\\x0a1", "\\x01");
		
		HDRow rowA = new HDRow();
		rowA.put("cf1\\xFF", rowAcf1);

		
		HDTable hdata = new HDTable();
		hdata.addRow("\\x80rowA", rowA);
		return hdata;
		
	}
	

	@Test
	public void test3() throws IOException  {
		HDTable d = this.buildTestSet2();
		String s1 = d.toJsonString();
		HDTable d2 = HDTable.fromJson(s1);
		String s2 = d2.toJsonString();
		Assert.assertEquals(s1, s2);
		/*
		System.out.println(s1);
		System.out.println("-------------------------------");
		System.out.println(s2);
		*/
	}

	@Test
	public void test4() throws IOException  {
		HDTable d1 = this.buildTestSet2();
		HDTable d2 = HDTable.fromJson(d1.toJson());
		Assert.assertEquals(d1.toJson(), d2.toJson());
		
		Assert.assertTrue(d2.containsKey("\\x80rowA"));
		Assert.assertArrayEquals(new byte[] { (byte)0x80 },  Bytes.toBytesBinary("\\x80"));
		String x = d2.get( "\\x80rowA").get("cf1\\xFF").get("\\x0a1");
		Assert.assertEquals((byte)0x01,  Bytes.toBytesBinary(x)[0]);
		
		/*
		System.out.println(d2.toJsonString());
		System.out.println("-------------------------------");
		System.out.println(d2.toJson());
		*/
		System.out.println(d2.toJsonString());
		System.out.println("-------------------------------");
		System.out.println(d2.toJson());
	}
}

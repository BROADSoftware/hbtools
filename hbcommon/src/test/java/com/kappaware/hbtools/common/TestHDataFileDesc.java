package com.kappaware.hbtools.common;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;


public class TestHDataFileDesc {

	public HDataFileDesc buildTestSet() {
		Map<String, String> rowAcf1 = new HashMap<String, String>();
		rowAcf1.put("rowAcf1Col_a", "rowAcf1Value_a");
		rowAcf1.put("rowAcf1Col_b", "rowAcf1Value_b");
		rowAcf1.put("rowAcf1Col_c", "rowAcf1Value_c");
		//rowAcf1.put("rowAcf1Col\\x0a_d", "rowAcf1Value\\x09_d");
		Map<String, String> rowAcf2 = new HashMap<String, String>();
		rowAcf2.put("rowAcf2Col_a", "rowAcf2Value_a");
		rowAcf2.put("rowAcf2Col_b", "rowAcf2Value_b");
		rowAcf2.put("rowAcf2Col_c", "rowAcf2Value_c");

		Map<String, Map<String, String>> rowA = new HashMap<String, Map<String, String>>();
		rowA.put("cf1", rowAcf1);
		rowA.put("cf2", rowAcf2);
		
		
		Map<String, String> rowBcf1 = new HashMap<String, String>();
		rowBcf1.put("rowBcf1Col_a", "rowBcf1Value_a");
		rowBcf1.put("rowBcf1Col_b", "rowBcf1Value_b");
		Map<String, String> rowBcf2 = new HashMap<String, String>();
		rowBcf2.put("rowBcf2Col_a", "rowBcf2Value_a");
		rowBcf2.put("rowBcf2Col_b", "rowBcf2Value_b");
		rowBcf2.put("rowBcf2Col_c", "rowBcf2Value_c");

		Map<String, Map<String, String>> rowB = new HashMap<String, Map<String, String>>();
		rowB.put("cf1", rowBcf1);
		rowB.put("cf2", rowBcf2);

		
		Map<String, String> rowCcf1 = new HashMap<String, String>();
		rowCcf1.put("rowCcf1Col_a", "rowCcf1Value_a");
		rowCcf1.put("rowCcf1Col_b", "rowCcf1Value_b");
		rowCcf1.put("rowCcf1Col_c", "rowCcf1Value_c");
		
		Map<String, Map<String, String>> rowC = new HashMap<String, Map<String, String>>();
		rowC.put("cf1", rowCcf1);

		HDataFileDesc hdata = new HDataFileDesc();
		hdata.addRow("rowA", rowA);
		hdata.addRow("rowB", rowB);
		hdata.addRow("rowC", rowC);
		
		return hdata;
		
	}

	@Test
	public void test1() throws IOException  {
		HDataFileDesc d = this.buildTestSet();
		String s1 = d.toJsonString();
		HDataFileDesc d2 = HDataFileDesc.fromJson(s1);
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
		HDataFileDesc d1 = this.buildTestSet();
		HDataFileDesc d2 = HDataFileDesc.fromJson(d1.toJsonStringSpec());

		Assert.assertEquals(d1.toJsonStringSpec(), d2.toJsonStringSpec());
		/*
		System.out.println(d2.toJsonString());
		System.out.println("-------------------------------");
		System.out.println(d2.toJsonStringSpec());
		*/
	}

	public HDataFileDesc buildTestSet2() {
		Map<String, String> rowAcf1 = new HashMap<String, String>();
		rowAcf1.put("\\x0a1", "\\x01");
		
		Map<String, Map<String, String>> rowA = new HashMap<String, Map<String, String>>();
		rowA.put("cf1\\xFF", rowAcf1);

		
		HDataFileDesc hdata = new HDataFileDesc();
		hdata.addRow("\\x80rowA", rowA);
		return hdata;
		
	}
	

	@Test
	public void test3() throws IOException  {
		HDataFileDesc d = this.buildTestSet2();
		String s1 = d.toJsonString();
		HDataFileDesc d2 = HDataFileDesc.fromJson(s1);
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
		HDataFileDesc d1 = this.buildTestSet2();
		HDataFileDesc d2 = HDataFileDesc.fromJson(d1.toJsonStringSpec());
		Assert.assertEquals(d1.toJsonStringSpec(), d2.toJsonStringSpec());
		
		Assert.assertTrue(d2.containsKey("\\x80rowA"));
		Assert.assertArrayEquals(new byte[] { (byte)0x80 },  Bytes.toBytesBinary("\\x80"));
		String x = d2.get( "\\x80rowA").get("cf1\\xFF").get("\\x0a1");
		Assert.assertEquals((byte)0x01,  Bytes.toBytesBinary(x)[0]);
		
		/*
		System.out.println(d2.toJsonString());
		System.out.println("-------------------------------");
		System.out.println(d2.toJsonStringSpec());
		*/
	}
}

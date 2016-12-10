package com.kappaware.hbtools.common;


import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.kappaware.hbtools.common.HDataFileString.ColFamString;
import com.kappaware.hbtools.common.HDataFileString.RowString;
import com.kappaware.hbtools.common.HDataFileString.TableString;


public class TestHDataFileString {

	public TableString buildTestSet() {
		ColFamString rowAcf1 = new ColFamString();
		rowAcf1.put("rowAcf1Col_a", "rowAcf1Value_a");
		rowAcf1.put("rowAcf1Col_b", "rowAcf1Value_b");
		rowAcf1.put("rowAcf1Col_c", "rowAcf1Value_c");
		//rowAcf1.put("rowAcf1Col\\x0a_d", "rowAcf1Value\\x09_d");
		ColFamString rowAcf2 = new ColFamString();
		rowAcf2.put("rowAcf2Col_a", "rowAcf2Value_a");
		rowAcf2.put("rowAcf2Col_b", "rowAcf2Value_b");
		rowAcf2.put("rowAcf2Col_c", "rowAcf2Value_c");

		RowString rowA = new RowString();
		rowA.put("cf1", rowAcf1);
		rowA.put("cf2", rowAcf2);
		
		
		ColFamString rowBcf1 = new ColFamString();
		rowBcf1.put("rowBcf1Col_a", "rowBcf1Value_a");
		rowBcf1.put("rowBcf1Col_b", "rowBcf1Value_b");
		ColFamString rowBcf2 = new ColFamString();
		rowBcf2.put("rowBcf2Col_a", "rowBcf2Value_a");
		rowBcf2.put("rowBcf2Col_b", "rowBcf2Value_b");
		rowBcf2.put("rowBcf2Col_c", "rowBcf2Value_c");

		RowString rowB = new RowString();
		rowB.put("cf1", rowBcf1);
		rowB.put("cf2", rowBcf2);

		
		ColFamString rowCcf1 = new ColFamString();
		rowCcf1.put("rowCcf1Col_a", "rowCcf1Value_a");
		rowCcf1.put("rowCcf1Col_b", "rowCcf1Value_b");
		rowCcf1.put("rowCcf1Col_c", "rowCcf1Value_c");
		
		RowString rowC = new RowString();
		rowC.put("cf1", rowCcf1);

		TableString hdata = new TableString();
		hdata.addRow("rowA", rowA);
		hdata.addRow("rowB", rowB);
		hdata.addRow("rowC", rowC);
		
		return hdata;
		
	}

	@Test
	public void test1() throws IOException  {
		TableString d = this.buildTestSet();
		String s1 = d.toJsonString();
		TableString d2 = TableString.fromJson(s1);
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
		TableString d1 = this.buildTestSet();
		TableString d2 = TableString.fromJson(d1.toJson());

		Assert.assertEquals(d1.toJson(), d2.toJson());
		/*
		System.out.println(d2.toJsonString());
		System.out.println("-------------------------------");
		System.out.println(d2.toJson());
		*/
		System.out.println(d2.toJson());
	}

	public TableString buildTestSet2() {
		ColFamString rowAcf1 = new ColFamString();
		rowAcf1.put("\\x0a1", "\\x01");
		
		RowString rowA = new RowString();
		rowA.put("cf1\\xFF", rowAcf1);

		
		TableString hdata = new TableString();
		hdata.addRow("\\x80rowA", rowA);
		return hdata;
		
	}
	

	@Test
	public void test3() throws IOException  {
		TableString d = this.buildTestSet2();
		String s1 = d.toJsonString();
		TableString d2 = TableString.fromJson(s1);
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
		TableString d1 = this.buildTestSet2();
		TableString d2 = TableString.fromJson(d1.toJson());
		Assert.assertEquals(d1.toJson(), d2.toJson());
		
		Assert.assertTrue(d2.containsKey("\\x80rowA"));
		Assert.assertArrayEquals(new byte[] { (byte)0x80 },  Bytes.toBytesBinary("\\x80"));
		String x = d2.get( "\\x80rowA").get("cf1\\xFF").get("\\x0a1");
		Assert.assertEquals((byte)0x01,  Bytes.toBytesBinary(x)[0]);
		
		/*
		System.out.println(d2.toJsonString());
		System.out.println("-------------------------------");
		System.out.println(d2.toJsonString());
		*/
	}
}

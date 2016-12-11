package com.kappaware.hbload;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestByteArray {
	
	
	@Test
	public void test1() {
		
		byte[] key1 = new byte[] { 'k', 'e', 'y' };
		byte[] key2 = new byte[] { 'k', 'e', 'y' };
		byte[] value = new byte[] { 'v', 'a', 'l', 'u', 'e' };
		
		Map<byte[], byte[]> m = new HashMap<byte[], byte[]>();
		
		m.put(key1, value);
		byte[] value2 = (byte[])m.get(key2);
		Assert.assertNotNull(value2);
		
		
	}

}

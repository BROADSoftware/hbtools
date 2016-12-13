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
package com.kappaware.hbfaker;

import java.io.PrintStream;
import java.util.Random;
import java.util.zip.CRC32;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import com.kappaware.hbtools.common.HDataFile.HDFamily;
import com.kappaware.hbtools.common.HDataFile.HDRow;

public class PersonEngine {
	private PrintStream out;
	private Parameters parameters;
	Faker faker;

	public PersonEngine(PrintStream out, Parameters parameters) {
		this.out = out;
		this.parameters = parameters;
		faker = new Faker(new Random(parameters.getSeed()));
	}

	public void run() {
		out.print("{\n");
		String sep = " ";
		for (long i = 0; i < parameters.getCount(); i++) {
			HDRow row = new HDRow();

			byte b = 0;
			if (parameters.isDistributed() || parameters.isInjectHex()) {
				CRC32 chksum = new CRC32();
				chksum.update((int) i);
				b = (byte) (chksum.getValue() & 0xFF);
			}

			HDFamily colFamId = new HDFamily();
			Name fakerName = faker.name();
			colFamId.put("prefix", fakerName.prefix());
			colFamId.put("fname", fakerName.firstName());
			colFamId.put("lname", fakerName.lastName());
			colFamId.put("reg", String.format("%06d", i));
			if (parameters.isInjectHex()) {
				colFamId.put(String.format("key\\x%02X", i), String.format("\\x%02X%06d", b, i));
			}
			row.put("id", colFamId);

			HDFamily colFamJob = new HDFamily();
			colFamJob.put("title", fakerName.title());
			colFamJob.put("cpny", faker.company().name());
			row.put("job", colFamJob);

			out.print("   " + sep + "\"");
			if (parameters.isDistributed()) {
				out.print(String.format("\\\\x%02X%06d", b, i));
			} else {
				out.print(String.format("%06d", i));
			}
			out.print("\": ");
			out.print(row.toJson());
			out.print("\n");
			sep = ",";
		}
		out.print("}\n");
	}

}

package com.kappaware.hbfaker;

import java.io.PrintStream;
import java.util.Random;
import java.util.zip.CRC32;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import com.kappaware.hbtools.common.HDataFileString.ColFamString;
import com.kappaware.hbtools.common.HDataFileString.RowString;

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
			RowString row = new RowString();

			byte b = 0;
			if (parameters.isDistributed() || parameters.isInjectHex()) {
				CRC32 chksum = new CRC32();
				chksum.update((int) i);
				b = (byte) (chksum.getValue() & 0xFF);
			}

			ColFamString colFamId = new ColFamString();
			Name fakerName = faker.name();
			colFamId.put("prefix", fakerName.prefix());
			colFamId.put("fname", fakerName.firstName());
			colFamId.put("lname", fakerName.lastName());
			colFamId.put("reg", String.format("%06d", i));
			if (parameters.isInjectHex()) {
				colFamId.put(String.format("key\\x%02X", i), String.format("\\x%02X%06d", b, i));
			}
			row.put("id", colFamId);

			ColFamString colFamJob = new ColFamString();
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
package com.kappaware.hbfaker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.ParserHelpException;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) {
		log.info("hbfaker start");
		log.debug("hbfaker DEBUG enabled");
		try {
			main2(argv);
		} catch (ConfigurationException e) {
			log.error(e.getMessage());
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		} catch (ParserHelpException e) {
			System.out.println(e.getMessage());
			System.exit(0);
		}
	}

	static public void main2(String argv[]) throws ConfigurationException, ParserHelpException {

		Parameters parameters = new Parameters(argv);

		PrintStream out = System.out;
		if (parameters.getOutputFile() != null) {
			File file = new File(parameters.getOutputFile());
			try {
				out = new PrintStream(file);
			} catch (FileNotFoundException e) {
				throw new ConfigurationException(String.format("Unable to open '%s' for writing", file.getAbsolutePath()));
			}
		}
		PersonEngine pe = new PersonEngine(out, parameters);
		pe.run();
		out.flush();
		if(out != System.out) {
			out.close();
		}
			

	}

}

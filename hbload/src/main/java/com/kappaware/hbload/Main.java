package com.kappaware.hbload;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.HDataFileString.TableString;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) {
		log.info("hbload start");
		log.debug("hbload DEBUG enabled");
		try {
			main2(argv);
		} catch (ConfigurationException | IOException e) {
			log.error(e.getMessage());
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		} 
	}
	
	static public void main2(String[] argv) throws ConfigurationException, JsonParseException, JsonMappingException, IOException {
		Parameters parameters = new Parameters(argv);
		File file = new File(parameters.getInputFile());
		if (!file.canRead()) {
			throw new ConfigurationException(String.format("Unable to open '%s' for reading", file.getAbsolutePath()));
		}
		TableString data = TableString.fromFile(file);
		//TableBinary data = new TableBinary(dataString);
		Engine engine = new Engine(parameters, data);
		engine.run();
	}

}

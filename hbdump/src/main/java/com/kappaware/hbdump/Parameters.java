package com.kappaware.hbdump;

import com.kappaware.hbtools.common.BaseParameters;
import com.kappaware.hbtools.common.ConfigurationException;

import joptsimple.OptionSpec;

public class Parameters extends BaseParameters {
	private OptionSpec<String> OUTPUT_FILE_OPT;


	public Parameters(String[] argv) throws ConfigurationException {
		super();
		OUTPUT_FILE_OPT = parser.accepts("outputFile", "HBase JSON output data file (Default to stdout)").withRequiredArg().describedAs("output file").ofType(String.class);

		this.parse(argv);
	}
	
	// -------------------------------------------
	public String getOutputFile() {
		return result.valueOf(OUTPUT_FILE_OPT);
	}
	
}

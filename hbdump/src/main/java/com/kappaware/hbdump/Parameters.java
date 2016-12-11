package com.kappaware.hbdump;

import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.HBaseParameters;
import com.kappaware.hbtools.common.ParserHelpException;

import joptsimple.OptionSpec;

public class Parameters extends HBaseParameters {
	private OptionSpec<String> OUTPUT_FILE_OPT;

	public Parameters(String[] argv) throws ConfigurationException, ParserHelpException {
		super();
		OUTPUT_FILE_OPT = parser.accepts("outputFile", "HBase JSON output data file (Default to stdout)").withRequiredArg().describedAs("output file").ofType(String.class);

		this.parse(argv);
	}
	
	// -------------------------------------------
	public String getOutputFile() {
		return result.valueOf(OUTPUT_FILE_OPT);
	}
}

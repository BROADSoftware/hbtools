package com.kappaware.hbload;

import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.HBaseParameters;

import joptsimple.OptionSpec;

public class Parameters extends HBaseParameters {
	private OptionSpec<String> INPUT_FILE_OPT;
	private OptionSpec<?> DONT_ADD_ROW_OPT;
	private OptionSpec<?> DEL_ROW_OPT;
	private OptionSpec<?> DONT_ADD_VALUE_OPT;
	private OptionSpec<?> DEL_VALUE_OPT;
	private OptionSpec<?> UPD_VALUE_OPT;

	public Parameters(String[] argv) throws ConfigurationException {
		super();
		INPUT_FILE_OPT = parser.accepts("inputFile", "HBase JSON data file").withRequiredArg().describedAs("input file").ofType(String.class).required();
		
		DONT_ADD_ROW_OPT = parser.accepts("dontAddRow", "Do not add row in table if does not exist");
		DEL_ROW_OPT = parser.accepts("delRows", "Delete rows in table if not defined in file");

		DONT_ADD_VALUE_OPT = parser.accepts("dontAddValue", "Do not add column value if not existing in a row");
		DEL_VALUE_OPT = parser.accepts("delValue", "Delete column value in row if not defined in file");
		UPD_VALUE_OPT = parser.accepts("updValue", "Update column value in row if different");

		this.parse(argv);
	}
	
	
	
	// -------------------------------------------
	
	public String getInputFile() {
		return result.valueOf(INPUT_FILE_OPT);
	}

	public boolean isAddRow() {
		return !result.has(DONT_ADD_ROW_OPT);
	}

	public boolean isDelRow() {
		return result.has(DEL_ROW_OPT);
	}
	
	public boolean isAddValue() {
		return !result.has(DONT_ADD_VALUE_OPT);
	}
	
	public boolean isDelValue() {
		return result.has(DEL_VALUE_OPT);
	}
	
	public boolean isUpdValue() {
		return result.has(UPD_VALUE_OPT);
	}
	
}

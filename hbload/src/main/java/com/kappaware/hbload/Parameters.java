package com.kappaware.hbload;

import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.HBaseParameters;

import joptsimple.OptionSpec;

public class Parameters extends HBaseParameters {
	private OptionSpec<String> INPUT_FILE_OPT;
	private OptionSpec<Boolean> ADD_ROW_OPT;
	private OptionSpec<Boolean> DEL_ROW_OPT;
	private OptionSpec<Boolean> ADD_COL_OPT;
	private OptionSpec<Boolean> DEL_COL_OPT;
	private OptionSpec<Boolean> UPD_COL_OPT;

	public Parameters(String[] argv) throws ConfigurationException {
		super();
		INPUT_FILE_OPT = parser.accepts("inputFile", "HBase JSON data file").withRequiredArg().describedAs("input file").ofType(String.class).required();
		ADD_ROW_OPT = parser.accepts("addRow", "Add row in table if does not exist").withRequiredArg().describedAs("yes|no").ofType(Boolean.class).defaultsTo(true);
		DEL_ROW_OPT = parser.accepts("delRow", "Delete row in table if not defined in file").withRequiredArg().describedAs("yes|no").ofType(Boolean.class).defaultsTo(false);

		ADD_COL_OPT = parser.accepts("addCol", "Add column value if not existing in a row").withRequiredArg().describedAs("yes|no").ofType(Boolean.class).defaultsTo(true);
		DEL_COL_OPT = parser.accepts("delCol", "Delete column value in row if not defined in file").withRequiredArg().describedAs("yes|no").ofType(Boolean.class).defaultsTo(false);
		UPD_COL_OPT = parser.accepts("updRow", "Update column value in row if different").withRequiredArg().describedAs("yes|no").ofType(Boolean.class).defaultsTo(false);

		this.parse(argv);
	}
	
	
	
	// -------------------------------------------
	
	public String getInputFile() {
		return result.valueOf(INPUT_FILE_OPT);
	}

	public boolean isAddRow() {
		return result.valueOf(ADD_ROW_OPT);
	}

	public boolean isDelRow() {
		return result.valueOf(DEL_ROW_OPT);
	}
	
	public boolean isAddCol() {
		return result.valueOf(ADD_COL_OPT);
	}
	
	public boolean isDelCol() {
		return result.valueOf(DEL_COL_OPT);
	}
	
	public boolean isUpdCol() {
		return result.valueOf(UPD_COL_OPT);
	}
	
}

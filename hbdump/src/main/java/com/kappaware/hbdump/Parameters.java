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

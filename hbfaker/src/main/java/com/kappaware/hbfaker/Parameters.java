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

import com.kappaware.hbtools.common.BaseParameters;
import com.kappaware.hbtools.common.ConfigurationException;
import com.kappaware.hbtools.common.ParserHelpException;

import joptsimple.OptionSpec;

public class Parameters extends BaseParameters {
	private OptionSpec<String> OUTPUT_FILE_OPT;
	private OptionSpec<Long> SEED_OPT;
	private OptionSpec<Long> COUNT_OPT;
	private OptionSpec<?> DISTRIBUTED_OPT;
	private OptionSpec<?> INJECT_HEX_OPT;


	public Parameters(String[] argv) throws ConfigurationException, ParserHelpException {
		super();
		OUTPUT_FILE_OPT = parser.accepts("outputFile", "HBase JSON output data file (Default to stdout)").withRequiredArg().describedAs("output file").ofType(String.class);
		SEED_OPT = parser.accepts("seed", "Random starting point").withRequiredArg().describedAs("seed").ofType(Long.class).defaultsTo(1L);
		COUNT_OPT = parser.accepts("count", "Number of record").withRequiredArg().describedAs("count").ofType(Long.class).defaultsTo(10L);
		DISTRIBUTED_OPT = parser.accepts("distributed", "Inject binary random byte as key prefix");
		INJECT_HEX_OPT = parser.accepts("injectHex", "Inject some hex value in data");

		this.parse(argv);
	}
	
	// -------------------------------------------
	public String getOutputFile() {
		return result.valueOf(OUTPUT_FILE_OPT);
	}
	
	public Long getSeed() {
		return result.valueOf(SEED_OPT);
	}

	public Long getCount() {
		return result.valueOf(COUNT_OPT);
	}
	
	public boolean isDistributed() {
		return result.has(DISTRIBUTED_OPT);
	}
	
	public boolean isInjectHex() {
		return result.has(INJECT_HEX_OPT);
	}
	
	
}

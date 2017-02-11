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
package com.kappaware.hbtools.common;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionSpec;

public class HBaseParameters extends BaseParameters {
	static Logger log = LoggerFactory.getLogger(HBaseParameters.class);

	private OptionSpec<String> NAMESPACE_OPT;
	private OptionSpec<String> TABLE_OPT;
	private OptionSpec<String> CONFIG_FILES_OPT;
	private OptionSpec<String> PRINCIPAL_OPT;
	private OptionSpec<String> KEYTAB_OPT;
	private OptionSpec<String> DUMP_CONFIG_FILE_OPT;
	private OptionSpec<Integer> CLIENT_RETRIES_OPT;
	
	
	
	public HBaseParameters() {
		super();

		NAMESPACE_OPT = parser.accepts("namespace", "HBase namespace").withRequiredArg().describedAs("mynamespace").ofType(String.class).defaultsTo("default");
		TABLE_OPT = parser.accepts("table", "HBase table").withRequiredArg().describedAs("mytable").ofType(String.class).required();

		CONFIG_FILES_OPT = parser.accepts("configFile", "Configuration file (xxx-site.xml). May be specified several times").withRequiredArg().describedAs("xxxx-site.xml").ofType(String.class);
		
		PRINCIPAL_OPT = parser.accepts("principal", "Kerberos principal").withRequiredArg().describedAs("principal").ofType(String.class);
		KEYTAB_OPT = parser.accepts("keytab", "Keytyab file path").withRequiredArg().describedAs("keytab_file").ofType(String.class);

		DUMP_CONFIG_FILE_OPT = parser.accepts("dumpConfigFile", "Debugging purpose: All HBaseConfiguration will be dumped in this file").withRequiredArg().describedAs("dump_file").ofType(String.class);

		CLIENT_RETRIES_OPT = parser.accepts("clientRetries", "Number of connection attemps before failure").withRequiredArg().describedAs("nbr_retries").ofType(Integer.class).defaultsTo(6);
	
	}


	// --------------------------------------------------------------------------

	public String getNamespace() {
		return result.valueOf(NAMESPACE_OPT);
	}

	public String getTable() {
		return result.valueOf(TABLE_OPT);
	}
	

	public String getPrincipal() {
		return result.valueOf(PRINCIPAL_OPT);
	}

	public String getKeytab() {
		return result.valueOf(KEYTAB_OPT);
	}

	public String getDumpConfigFile() {
		return result.valueOf(DUMP_CONFIG_FILE_OPT);
	}

	public List<String> getConfigFiles() {
		return result.valuesOf(CONFIG_FILES_OPT);
	}

	public int getClientRetries() {
		return result.valueOf(CLIENT_RETRIES_OPT);
	}

}

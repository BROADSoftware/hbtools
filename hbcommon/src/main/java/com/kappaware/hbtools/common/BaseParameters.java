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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class BaseParameters {
	static Logger log = LoggerFactory.getLogger(BaseParameters.class);

	protected OptionParser parser;
	protected OptionSet result;

	private OptionSpec<String> ZOOKEEPER_OPT;
	private OptionSpec<String> ZNODE_PARENT_OPT;
	private OptionSpec<String> NAMESPACE_OPT;
	private OptionSpec<String> TABLE_OPT;

	@SuppressWarnings("serial")
	private static class MyOptionException extends Exception {
		public MyOptionException(String message) {
			super(message);
		}
	}

	public BaseParameters() {
		parser = new OptionParser();
		parser.formatHelpWith(new BuiltinHelpFormatter(120, 2));

		ZOOKEEPER_OPT = parser.accepts("zookeeper", "Comma separated values of Zookeeper nodes").withRequiredArg().describedAs("zk1:2181,ek2:2181").ofType(String.class).required();
		ZNODE_PARENT_OPT = parser.accepts("znodeParent", "HBase znode parent (Default: '/hbase')").withRequiredArg().describedAs("znodeParent").ofType(String.class).defaultsTo("default");
		NAMESPACE_OPT = parser.accepts("namespace", "HBase namespace (Default: 'default')").withRequiredArg().describedAs("mynamespace").ofType(String.class).defaultsTo("default");
		TABLE_OPT = parser.accepts("table", "HBase table").withRequiredArg().describedAs("mytable").ofType(String.class).required();
	}

	public void parse(String[] argv) throws ConfigurationException {
		try {
			result = parser.parse(argv);
			if (result.nonOptionArguments().size() > 0 && result.nonOptionArguments().get(0).toString().trim().length() > 0) {
				throw new MyOptionException(String.format("Unknow option '%s'", result.nonOptionArguments().get(0)));
			}
		} catch (OptionException | MyOptionException t) {
			throw new ConfigurationException(usage(t.getMessage()));
		}
	}

	private String usage(String err) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(baos);
		if (err != null) {
			pw.print(String.format("\n\n * * * * * ERROR: %s\n\n", err));
		}
		try {
			parser.printHelpOn(pw);
		} catch (IOException e) {
		}
		pw.flush();
		pw.close();
		return baos.toString();
	}

	// --------------------------------------------------------------------------


	public String getZookeeper() {
		return result.valueOf(ZOOKEEPER_OPT);
	}

	public String getZnodeParent() {
		return result.valueOf(ZNODE_PARENT_OPT);
	}

	public String getNamespace() {
		return result.valueOf(NAMESPACE_OPT);
	}

	public String getTable() {
		return result.valueOf(TABLE_OPT);
	}
}

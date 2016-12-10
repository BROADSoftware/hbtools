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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.OptionSpec;

public class HBaseParameters extends BaseParameters {
	static Logger log = LoggerFactory.getLogger(HBaseParameters.class);

	private OptionSpec<String> ZOOKEEPER_OPT;
	private OptionSpec<String> ZNODE_PARENT_OPT;
	private OptionSpec<String> NAMESPACE_OPT;
	private OptionSpec<String> TABLE_OPT;


	public HBaseParameters() {
		super();

		ZOOKEEPER_OPT = parser.accepts("zookeeper", "Comma separated values of Zookeeper nodes").withRequiredArg().describedAs("zk1:2181,zk2:2181").ofType(String.class).required();
		ZNODE_PARENT_OPT = parser.accepts("znodeParent", "HBase znode parent").withRequiredArg().describedAs("znodeParent").ofType(String.class).defaultsTo("/hbase");
		NAMESPACE_OPT = parser.accepts("namespace", "HBase namespace").withRequiredArg().describedAs("mynamespace").ofType(String.class).defaultsTo("default");
		TABLE_OPT = parser.accepts("table", "HBase table").withRequiredArg().describedAs("mytable").ofType(String.class).required();
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

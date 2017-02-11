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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
	static Logger log = LoggerFactory.getLogger(Utils.class);

	public static boolean isNullOrEmpty(String s) {
		return (s == null || s.trim().length() == 0);
	}

	public static boolean hasText(String s) {
		return (s != null && s.trim().length() > 0);
	}


	static public void dumpConfiguration(Configuration conf, String dumpFile) throws IOException {
		Writer out = null;
		try {
			out = new BufferedWriter(new FileWriter(dumpFile, false));
			/*
			Map<String, String> result = conf.getValByRegex(".*");
			for (String s : result.keySet()) {
				out.write(String.format("%s -> %s\n", s, result.get(s)));
			}
			*/
			Iterator<Map.Entry<String, String>> it = conf.iterator();
			while(it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				out.write(String.format("%s -> %s\n", entry.getKey(), entry.getValue()));
			}
			//Configuration.dumpConfiguration(conf, out);
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	

	public static Configuration buildHBaseConfiguration(HBaseParameters parameters) throws ConfigurationException, IOException {
		Configuration config = HBaseConfiguration.create();
		for(String cf: parameters.getConfigFiles()) {
			File f = new File(cf);
			if(!f.canRead()) {
				throw new ConfigurationException(String.format("Unable to read file '%s'", cf));
			}
			log.debug(String.format("Will load '%s'", cf));
			config.addResource(new Path(cf));
		}
		config.set("hbase.client.retries.number", Integer.toString(parameters.getClientRetries()));
		//config.reloadConfiguration();
		if (Utils.hasText(parameters.getDumpConfigFile())) {
			Utils.dumpConfiguration(config, parameters.getDumpConfigFile());
		}
		if (Utils.hasText(parameters.getKeytab()) && Utils.hasText(parameters.getPrincipal())) {
			// Check if keytab file exists and is readable
			File f = new File(parameters.getKeytab());
			if(! f.canRead()) {
				throw new ConfigurationException(String.format("Unable to read keytab file: '%s'", parameters.getKeytab()));
			}
			UserGroupInformation.setConfiguration(config);
			if (!UserGroupInformation.isSecurityEnabled()) {
				throw new ConfigurationException("Security is not enabled in core-site.xml while Kerberos principal and keytab are provided.");
			}
			try {
				UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(parameters.getPrincipal(), parameters.getKeytab());
				UserGroupInformation.setLoginUser(userGroupInformation);
			} catch (Exception e) {
				throw new ConfigurationException(String.format("Kerberos: Unable to authenticate with principal='%s' and keytab='%s': %s.", parameters.getPrincipal(), parameters.getKeytab(), e.getMessage()));
			}
		}
		return config;
	}
	

}

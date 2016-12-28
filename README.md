# hbtools

hbtools is a project hosting tools aimed to dump and load datatset from/to Apache HBase.

* hddump to dump a full hbase table content in json format.
* hbload to load a json dataset to an HBase table in an idempotent way. 

WARNING: hbtools are not intended to work on 'big' dataset. hbtools is intended to be use in development/POC context, for test dataset, or in application deployment where some tables need to be populated with an initial bunch of data.

While hbdump works with a streaming pattern, as such will be able to dump data whatever volume is, hbload fully load its dataset in memory, thus limiting the volume it will be able to manage.

*** 
## hbdump

hbdump is provided as an all included jar (uberJar or fatJar)  on the [release pages](https://github.com/BROADSoftware/hbtools/releases).

There are several package, which differ by HBase and Hadoop library version. Pick up the one appropriate for your context.

You can launch hbdump by following command:

	java -jar hbdump_XXXXX-0.1.0.jar --help

Which should give the following output:

	Option (* = required)              Description
	---------------------              -----------
	--help                             Display this usage message
	--namespace <mynamespace>          HBase namespace (default: default)
	--outputFile <output file>         HBase JSON output data file (Default to stdout)
	* --table <mytable>                HBase table
	--znodeParent <znodeParent>        HBase znode parent (default: /hbase)
	* --zookeeper <zk1:2181,zk2:2181>  Comma separated values of Zookeeper nodes

Here is a short explanation of the options:

* `table:` The HBase table you want to dump.

* `namespaces:` The namespace of the table you want to dump.

* `outputFile:` The output file.

* `zookeeper:` Must be filled with the zookeeper's quorum of your target cluster. 

* `znodeParent:` Must match the value of the property `zookeeper.znode.parent` in `hbase-site.xml`. Should be `/hbase` in most case, `/hbase-unsecure` on some HDP cluster.

*** 
## hbload

hbload is provided as an all included jar (uberJar or fatJar)  on the [release pages](https://github.com/BROADSoftware/hbtools/releases).

There are several package, which differ by HBase and Hadoop library version. Pick up the one appropriate for your context.

You can launch hbload by following command:

	java -jar hbload_XXXXX-0.1.0.jar --help

Which should give the following output:

	Option (* = required)              Description
	---------------------              -----------
	--delRows                          Delete rows in table if not defined in file
	--delValues                        Delete column value in row if not defined in file
	--dontAddRow                       Do not add row in table if does not exist
	--dontAddValue                     Do not add column value if not existing in a row
	--help                             Display this usage message
	* --inputFile <input file>         HBase JSON data file
	--namespace <mynamespace>          HBase namespace (default: default)
	* --table <mytable>                HBase table
	--updValues                        Update column value in row if different
	--znodeParent <znodeParent>        HBase znode parent (default: /hbase)
	* --zookeeper <zk1:2181,zk2:2181>  Comma separated values of Zookeeper nodes


Here is a short explanation of the options:

* `table:` The HBase table you want to load into.

* `namespaces:` The namespace of the table you want to load into.

* `outputFile:` The input file. See below for its format

* `zookeeper:` Must be filled with the zookeeper's quorum of your target cluster. 

* `znodeParent:` Must match the value of the property `zookeeper.znode.parent` in `hbase-site.xml`. Should be `/hbase` in most case, `/hbase-unsecure` on some HDP cluster.

* `delRows:` See explanation below.

* `delValues:` See explanation below. 

* `dontAddRow:` See explanation below. 

* `dontAddValue:` See explanation below. 

* `updValues:` See explanation below.

### Idempotency

hbload is an idempotent tools. This means if you re-launch it on the same table with the same input data, this will have no effect.

### Action on pre-existing data

If the hbase table already contains some data, the default behavior is 'Just add, don't delete, don't modify'. Such behavior may be modified by command line switches.

* Un-existing row will be added. (Un-existing means there is no row with this rowkey present in the table). If you want to prevent this, set `--dontAddRow`.

* Row existing in the table but not in the input file will NOT be deleted. If you want to delete them, set `--delRows`.

* If a row already exists and is identical, it will not be modified.

* If a row exists in the table, but the input file provide more columns:values, the missing column:value will be added. If you want to prevent this, set `--dontAddValue`

* If a row exists, but the input file provide less column:value, the column:value will NOT be deleted. If you want to delete them, set `--delValues`.

* If a row exists but some value for an existing qualifier differs from the one in the input file, value will NOT be updated. If you want to update them, use `--updValues`.

If you set the switch combination `--dontAddRow --dontAddValue`, then hbload will do nothing.

If you set the switch combination `--delRows --delValue  --updValues`, then the content of the table will be adjusted to be fully identical to the content of the input file.  

** WARNING: hbload does not dynamically create HBase column family. All column family referenced in the input file must exist. Or an error will be generated.** 

***
## File format

The file format used by hbtools is a json with the following form:

	{
	    { "rowKey1": { "colFamily1": { "qualifier1": "value1", "qualifier2": "value2", ...}, "colFamily2": { ... }, ...}, 
	    { "rowKey2": { "colFamily1": { "qualifier1": "value1", "qualifier2": "value2", ...}, "colFamily2": { ... }, ...},
	    ...
	} 

For example:

	{
	    "000000": { "id": { "fname": "Delpha", "lname": "Dickinson", "prefix": "Mr.", "reg": "000000" }, "job": { "cpny": "Barton, Barton and Barton", "title": "Human Branding Officer" } }
	   ,"000001": { "id": { "fname": "Alvina", "lname": "Schulist", "prefix": "Dr.", "reg": "000001" }, "job": { "cpny": "Hilll, Hilll and Hilll", "title": "Investor Brand Coordinator" } }
	   ,"000002": { "id": { "fname": "Berniece", "lname": "Bahringer", "prefix": "Mrs.", "reg": "000002" }, "job": { "cpny": "Eichmann-Eichmann", "title": "District Paradigm Coordinator" } }
	}


### Binary representation

Internally, HBase does not handle String, but byte\[\] (Array of bytes). So, there is a need to represent binary data in string representation. Choice has been made to use the escape convention of the HBase function Bytes.toByteBinary() and Bytes.toStringBinary(), using the '\xXX" notation, where XX is the hexadecimal code if the byte.

Note, the '\' itself must be escaped. For example, the binary code 1 will be represented by "\\\\0x01"

Also, note the hbdump will not escape printable characters, so, for example "7000003" and "\\\\x37000003" represents the same byte[] (x37 is the hexadecimal code of '7'). Same for "\\\\x2E000007" and ".000007"

***
## Limitations

* hbtools does not handle HBase cell's timestamp.

***
## License

    Copyright (C) 2016 BROADSoftware

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.





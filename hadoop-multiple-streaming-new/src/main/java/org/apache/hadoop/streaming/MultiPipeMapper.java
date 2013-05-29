/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.streaming;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.URLDecoder;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.streaming.StreamUtil;
import org.apache.hadoop.util.StringUtils;

/**
 * A generic Mapper bridge. It delegates operations to an external program via
 * stdin and stdout.
 */
public class MultiPipeMapper extends MultiPipeMapRed implements Mapper {

	private boolean ignoreKey = false;
	private boolean skipping = false;

	private byte[] mapOutputFieldSeparator;
	private byte[] mapInputFieldSeparator;
	private int numOfMapOutputKeyFields = 1;
	
	// multipleoutputs
	private MultipleOutputs multipleOutputs;
	private Mapper test6mapper;
	private Mapper test7mapper;

	ArrayList<String> getPipeCommands(JobConf job) {
		ArrayList<String> commands = new ArrayList<String>();
		int length = Integer.parseInt(job
				.get("stream.map.num.streamprocessors"));
		for (int i = 0; i < length; i++) {
			String str = job.get("stream.map.streamprocessor"
					+ String.valueOf(i));
			if (str == null) {
				return null;
			}

			try {
				commands.add(URLDecoder.decode(str, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				System.err.println("stream.map.streamprocessor"
						+ String.valueOf(i) + " in jobconf not found");
			}
		}
		if (commands.size() == 0) {
			return null;
		}

		return commands;
	}

	String getPipeCommand(JobConf job) {
		return null;
	}

	boolean getDoPipe() {
		return true;
	}

	public void configure(JobConf job) {
		super.configure(job);
		// disable the auto increment of the counter. For streaming, no of
		// processed records could be different(equal or less) than the no of
		// records input.
		SkipBadRecords.setAutoIncrMapperProcCount(job, false);
		skipping = job.getBoolean("mapred.skip.on", false);
		String inputFormatClassName = job.getClass("mapred.input.format.class",
				TextInputFormat.class).getCanonicalName();
		ignoreKey = inputFormatClassName.equals(TextInputFormat.class
				.getCanonicalName());
		
		// multipleoutputs
		multipleOutputs = new MultipleOutputs(job);
		String defaultPackage = this.getClass().getPackage().getName();

		try {
			test6mapper = (Mapper) StreamUtil.goodClassOrNull(job, "org.apache.hadoop.streaming.TestJavaMapper",
					defaultPackage).newInstance();
			test7mapper = (Mapper) StreamUtil.goodClassOrNull(job, "org.apache.hadoop.streaming.TestJavaMapper",
					defaultPackage).newInstance();
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		try {
			mapOutputFieldSeparator = job.get(
					"stream.map.output.field.separator", "\t")
					.getBytes("UTF-8");
			mapInputFieldSeparator = job.get(
					"stream.map.input.field.separator", "\t").getBytes("UTF-8");
			numOfMapOutputKeyFields = job.getInt(
					"stream.num.map.output.key.fields", 1);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(
					"The current system does not support UTF-8 encoding!", e);
		}
	}

	// Do NOT declare default constructor
	// (MapRed creates it reflectively)

	public void map(Object key, Object value, OutputCollector output,
			Reporter reporter) throws IOException {
		if (outerrThreadsThrowable != null) {
			mapRedFinished();
			throw new IOException("MROutput/MRErrThread failed:"
					+ StringUtils.stringifyException(outerrThreadsThrowable));
		}
		try {
			// 1/4 Hadoop in
			numRecRead_++;
			maybeLogRecord();
			if (debugFailDuring_ && numRecRead_ == 3) {
				throw new IOException("debugFailDuring_");
			}

			// 2/4 Hadoop to Tool
			for (DataOutputStream out : clientOut_) {
				if (numExceptions_ == 0) {
					if (!this.ignoreKey) {
						write(key, out);
						out.write(getInputSeparator());
					}
					write(value, out);
					out.write('\n');
					if (skipping) {
						// flush the streams on every record input if running in
						// skip mode
						// so that we don't buffer other records surrounding a
						// bad
						// record.
						out.flush();
					}
				} else {
					numRecSkipped_++;
				}
			}
			
			// multipleoutputs
					
			PrefixOutputCollector test6Output = new PrefixOutputCollector("mapoutput/TEST6", multipleOutputs.getCollector("fromJava", reporter));
			PrefixOutputCollector test7Output = new PrefixOutputCollector("mapoutput/TEST7", multipleOutputs.getCollector("fromJava", reporter));
			// mapper 클래스가져와서 아래처럼 map 부르면될거같은데...
			test6mapper.map(key, value, test6Output, reporter);
			test7mapper.map(key, value, test7Output, reporter);
			
			// 클래스 생성하는 법을 모르니 일단 테스트
			//test6Output.collect(new Text("mapoutput/TEST6"), value);
			//test7Output.collect(new Text("mapoutput/TEST7"), value);
			
		} catch (IOException io) {
			numExceptions_++;
			if (numExceptions_ > 1
					|| numRecWritten_ < minRecWrittenToEnableSkip_) {
				// terminate with failure
				String msg = logFailure(io);
				appendLogToJobLog("failure");
				mapRedFinished();
				throw new IOException(msg);
			} else {
				// terminate with success:
				// swallow input records although the stream processor
				// failed/closed
			}
		}
	}

	public void close() {
		appendLogToJobLog("success");
		mapRedFinished();
	}

	byte[] getInputSeparator() {
		return mapInputFieldSeparator;
	}

	@Override
	byte[] getFieldSeparator() {
		return mapOutputFieldSeparator;
	}

	@Override
	int getNumOfKeyFields() {
		return numOfMapOutputKeyFields;
	}

}

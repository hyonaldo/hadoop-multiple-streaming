package org.apache.hadoop.streaming;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class PartitionByKeyField extends MultipleTextOutputFormat<Text, Text> {
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,
			String inputfilename) {
		return key.toString() + "/" + inputfilename;
	}

	@Override
	protected Text generateActualKey(Text key, Text value) {
		return null;
	}
}

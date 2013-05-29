package org.apache.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestJavaMapper extends MapReduceBase implements Mapper {

	public void map(Object key, Object value, OutputCollector output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		((Text) value).set(value.toString());
		output.collect(new Text("wanted"), value);

	}

}

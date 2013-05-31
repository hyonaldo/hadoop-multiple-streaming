package org.apache.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class PrefixOutputCollector<K2, V2> implements OutputCollector<K2, V2> {
	
	private OutputCollector output;
	private String prefix;
	public PrefixOutputCollector(String prefix, OutputCollector output) {
		// TODO Auto-generated constructor stub
		this.output = output;
		this.prefix = prefix;
	}
	
	public void collect(K2 key, V2 value) throws IOException {		
		// "\t" �� hadoop delimeter�� �ٲ� ��� ��
		((Text) value).set(key.toString() + "\t" + value.toString());
		((Text) key).set(prefix);
		this.output.collect(key, value);
	}

}

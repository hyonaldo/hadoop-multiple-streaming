hadoop-multiple-streaming
=========================
hadoop-multiple-streaming includes [Hadoop-Streaming](http://hadoop.apache.org/docs/r1.1.2/streaming.html).  
hadoop-multiple-streaming is an addition to the [Hadoop-Streaming](http://hadoop.apache.org/docs/r1.1.2/streaming.html) which is a utility that comes with the [Hadoop distribution](http://hadoop.apache.org/).  
This utility allows you to not only do [Hadoop-Streaming](http://hadoop.apache.org/docs/r1.1.2/streaming.html), but also create and run 'multiple' Map/Reduce jobs with any executable or script as the mappers and/or the reducers for 'one' input. For example:


    hadoop jar hadoop-multiple-streaming.jar \  
      -input    myInputDirs \  
      -multiple "outputDir1|mypackage.Mapper1|mypackage.Reducer1" \  
      -multiple "outputDir2|mapper2.sh|reducer2.sh" \  
      -multiple "outputDir3|mapper3.py|reducer3.py" \  
      -multiple "outputDir4|/bin/cat|/bin/wc" \  
      -libjars  "libDir/mypackage.jar" \
      -file     "libDir/mapper2.sh" \  
      -file     "libDir/mapper3.py" \  
      -file     "libDir/reducer2.sh" \  
      -file     "libDir/reducer3.py"

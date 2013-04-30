hadoop-multiple-streaming
=========================

hadoop-multiple-streaming is an addition to the Hadoop-Streaming which is a utility that comes with the Hadoop distribution.  
This utility allows you to not only do Hadoop-Streaming, but also create and run 'multiple' Map/Reduce jobs with any executable or script as the mappers and/or the reducers for 'one' input.  
hadoop-multiple-streaming includes Hadoop-Streaming. For example:


    hadoop jar hadoop-multiple-streaming.jar \  
      -libjars "mypackage.jar" \
      -input myInputDirs \  
      -multiple "outputDir1|mypackage.Mapper|mypackage.Reducer" \  
      -multiple "outputDir2|mapper2.sh|reducer2.sh" \  
      -multiple "outputDir3|mapper3.py|reducer3.py" \  
      -multiple "outputDir4|/bin/cat|/bin/wc"
      

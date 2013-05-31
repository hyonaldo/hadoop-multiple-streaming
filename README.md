hadoop-multiple-streaming
=========================
hadoop-multiple-streaming extends [Hadoop-Streaming](http://hadoop.apache.org/docs/r1.1.2/streaming.html) which is a utility that comes with the [Hadoop distribution](http://hadoop.apache.org/).  
This utility allows you to not only do [Hadoop-Streaming](http://hadoop.apache.org/docs/r1.1.2/streaming.html), but also create and run 'multiple' Map/Reduce jobs for 'one' input with any executable or scripts. For example:

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


This project is the maven project. So you can simply do maven build command for making hadoop-multiple-streaming.jar file.
In more detail, 'mvn clean package' command will compile source code and packaging to ${basedir}/target folder.

package org.apache.hadoop.streaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.RunningJob;

public class MultiStreamJobRunner{
	
	protected static final Log LOG = LogFactory.getLog(MultiStreamJobRunner.class
			.getName());
	protected String lastReport = null; 
	private String[] args;
	
	public MultiStreamJobRunner(String[] args) {
		this.args = args;
	}

	public void run() throws Exception {
		// multi map job 을 먼저 실행시키고 jobconf_ 를 얻어 red job 들을 thread 로 실행시켜준다.
		LOG.info("================= step1. MapJob =================");
		JobConf mapJobConf = runMapJob(args);
		LOG.info("================= step2. RedJob =================");
		List<RunningJob> runningJobs =  new ArrayList<RunningJob>();
		List<String> dirKeys =  new ArrayList<String>();
		List<String> red_args = new ArrayList<String>(Arrays.asList(args));
		String outputHome = "";
		// reduce를 위한 재 설정을 위한 작업
		for (int i = red_args.size() - 2; i >= 0; i--) { // remove 후 shift하기 때문에
															// 뒤에서 부터 찾아 삭제
			// output 찾기
			if (red_args.get(i).equals("-output")) {
				outputHome = red_args.remove(i + 1);
				red_args.remove(i);
				continue; // remove 후 shift하기 때문에
			}
			// input, mapred, outputformatter 옵션 삭제 -> 나중엔 outputformat map에서지우고 red에서 살려야함
			if (red_args.get(i).equals("-input")
					|| red_args.get(i).equals("-mapred")) {
				red_args.remove(i + 1); // 뒤에있는 input option의 value부터 삭제
				red_args.remove(i); // -input 삭제
			}
		}
		// input, mapper, reducer 설정
		int length = Integer.parseInt(mapJobConf
				.get("stream.reduce.num.streamprocessors"));
		for (int i = 0; i < length; i++) {
			dirKeys.add(URLDecoder.decode(mapJobConf.get("stream.dir.key"
					+ String.valueOf(i)), "UTF-8"));
			List<String> temp_args = new ArrayList<String>(red_args); // copy args list
			// input 설정
			temp_args.add("-input");
			temp_args.add(outputHome
					+ "/mapoutput/"
					+ dirKeys.get(i));
			// input 설정
			temp_args.add("-output");
			temp_args.add(outputHome
					+ "/"
					+ dirKeys.get(i));
			// mapper 설정
			temp_args.add("-mapper");
			temp_args.add(URLDecoder.decode(
					mapJobConf.get("stream.map.streamprocessor"
							+ String.valueOf(i)), "UTF-8"));
			// reducer 설정
			temp_args.add("-reducer");
			temp_args.add(URLDecoder.decode(
					mapJobConf.get("stream.reduce.streamprocessor"
							+ String.valueOf(i)), "UTF-8"));
			//print args
			LOG.info(dirKeys.get(i) + " Job Arguments\t" + temp_args);
			
			// run red jobs
			RedStreamJob redJob = new RedStreamJob();
			ToolRunner.run(redJob, (String[]) temp_args.toArray(new String[0]));
			runningJobs.add(redJob.getRunning_());
		}
		boolean completedAll = false;
		while (completedAll == false) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			if(reportComplete(runningJobs, dirKeys) >= runningJobs.size()){
				completedAll = true;
			}
		}
		reportSuccessful(runningJobs, dirKeys);
		LOG.info("================= Multi Stream Job complete =================");
	}
	public int reportComplete(List<RunningJob> runningJobs, List<String> dirKeys) throws IOException{
		String report = null;
		List<String> stringRunningJobs = new ArrayList<String>();
		List<String> stringCompletedJobs = new ArrayList<String>();
		for(int i = 0; i < runningJobs.size(); i++){
			if(runningJobs.get(i).isComplete() == true){
				//stringCompletedJobs.add(runningJobs.get(i).getID().toString());
				stringCompletedJobs.add(dirKeys.get(i));
			}else{
				//stringRunningJobs.add(runningJobs.get(i).getID().toString());
				stringRunningJobs.add(dirKeys.get(i));
			}
		}
		report = " Number Of Jobs Completed / Total\t" + String.valueOf(stringCompletedJobs.size()) + " / " + String.valueOf(runningJobs.size());
		if (!report.equals(this.lastReport)) {
			LOG.info(report);
			LOG.info(" Completed jobs\t" + stringCompletedJobs);
			LOG.info(" Running jobs\t" +  stringRunningJobs);
			this.lastReport = report;
		}
		return stringCompletedJobs.size();
	}
	public void reportSuccessful(List<RunningJob> runningJobs, List<String> dirKeys) throws IOException{
		boolean successfulAll = true;
		List<String> notSuccessful = new ArrayList<String>();
		for(int i = 0; i < runningJobs.size(); i++){
			if(runningJobs.get(i).isSuccessful() == false){
				successfulAll = false;
				//notSuccessful.add(runningJobs.get(i).getID().toString());
				notSuccessful.add(dirKeys.get(i));
			}
		}
		if(successfulAll){
			LOG.info(" All of Run Job Succeeded!\t");
		}else{
			LOG.error(" Some of Run Job Failed!\t=>\t" + notSuccessful);
		}
	}
	
	public JobConf runMapJob(String[] args) throws Exception {
		int returnStatus = 0;
		MapStreamJob mapJob = new MapStreamJob();
		returnStatus = ToolRunner.run(mapJob, args);
		if (returnStatus != 0) {
			LOG.error("Map Job Failed!");
			System.exit(returnStatus);
		}
		return mapJob.getJobConf();
	}

}

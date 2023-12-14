package com.spring.batch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HomeController {
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private Job job;
	
	@GetMapping("/xml-db")
	public void loadDataToXml() throws Exception{
		
		JobParameters jobParams = new JobParametersBuilder()
											.addLong("startAt", System.currentTimeMillis()).toJobParameters();
		
		jobLauncher.run(job, jobParams);
	}
	
}

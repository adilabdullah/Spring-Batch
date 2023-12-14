package com.spring.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import org.springframework.transaction.PlatformTransactionManager;

import com.spring.batch.Repository.CustomerRepo;
import com.spring.batch.entity.Customer;

@Configuration
public class BatchConfig {

	@Autowired
	CustomerRepo customerRepo;
	
//	StepBuilderFactory stepBuilder;
	
	
//	JobBuilderFactory  jobBuilder;
	// create Reader
	@Bean
	public FlatFileItemReader<Customer> customerReader()
	{
		FlatFileItemReader<Customer> itemReader=new FlatFileItemReader<>();
		itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
		itemReader.setName("csv-reader");
		itemReader.setLinesToSkip(1);
		itemReader.setLineMapper(lineMapper());
		return itemReader;
		
	}

	private LineMapper<Customer> lineMapper() {
		DefaultLineMapper<Customer> lineMapper=new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob");

		BeanWrapperFieldSetMapper<Customer> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(Customer.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

// create processor
	@Bean
	public CustomerProcessor customerProcess()
	{
		return new CustomerProcessor();
		
	}
	
	// create writer
	@Bean
	public RepositoryItemWriter<Customer> customerWriter()
	{
		RepositoryItemWriter<Customer> repoItemWriter=new RepositoryItemWriter<>();
		repoItemWriter.setRepository(customerRepo);
		repoItemWriter.setMethodName("save");
		return repoItemWriter;
	}
	
	// create step
	@Bean
	public Step step1(JobRepository jobRepo,PlatformTransactionManager transactionManager)
	{
			return new StepBuilder("step",jobRepo)
					.<Customer,Customer>chunk(10,transactionManager)
					.reader(customerReader())
					.processor(customerProcess())
					.writer(customerWriter())
					.build();
												
		
	}
	
	// create job
	@Bean
	public Job job(JobRepository jobRepository,Step step)
	{
		return new JobBuilder("customer-job",jobRepository)
				.flow(step)
				.end()
				.build();
	}  
	
}

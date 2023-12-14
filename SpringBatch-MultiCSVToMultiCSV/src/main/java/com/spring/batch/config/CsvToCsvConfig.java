package com.spring.batch.config;

import java.io.IOException;
import java.util.HashMap;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.transaction.PlatformTransactionManager;

import com.spring.batch.entity.Customer;


@Configuration
public class CsvToCsvConfig {

	
    Resource[] resources = null;

	@Bean
	public FlatFileItemReader<Customer> customerReader()
	{
		FlatFileItemReader<Customer> itemReader=new FlatFileItemReader<>();
	//	itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
		itemReader.setName("csv-reader");
		itemReader.setLinesToSkip(1);
		itemReader.setLineMapper(lineMapper());
		return itemReader;
		
	}
	
	@Bean
	public MultiResourceItemReader<Customer> multiResourceItemReader()
	{
		ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();  
        try {
			resources = patternResolver.getResources("file:D:/Jars/input/*.csv");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		MultiResourceItemReader<Customer> multiRead=new MultiResourceItemReader<Customer>();
		multiRead.setResources(resources);
		multiRead.setDelegate(customerReader());
		return multiRead;
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
	
	@Bean
	public FlatFileItemWriter<Customer> FileWriter()
	{
		FlatFileItemWriter<Customer> writer=new FlatFileItemWriter<Customer>();
		writer.setResource(new FileSystemResource("D:\\Jars\\data_output.csv"));
		DelimitedLineAggregator<Customer> fieldAggregator=new DelimitedLineAggregator<>();
		BeanWrapperFieldExtractor<Customer> fieldExtractor=new BeanWrapperFieldExtractor();
		fieldExtractor.setNames(new String[] {"id","firstName","lastName","email","gender","contactNo","country","dob"});
		fieldAggregator.setFieldExtractor(fieldExtractor);
		writer.setLineAggregator(fieldAggregator);
		return writer;
	}
	
	
	// create processor
		@Bean
		public CustomerProcessor customerProces()
		{
			return new CustomerProcessor();
			
		}


		@Primary
	@Bean
	public Step step2(JobRepository jobRepo1,PlatformTransactionManager transactionManager)
	{
			return new StepBuilder("step-2",jobRepo1)
					.<Customer,Customer>chunk(10,transactionManager)
					.reader(multiResourceItemReader())
					.processor(customerProces())
					.writer(FileWriter())
					.build();
												
		
	}
	// create job
		@Primary

	@Bean
	public Job jobs(JobRepository jobRepository1,Step step2)
	{
		return new JobBuilder("customer-jobs",jobRepository1)
				.flow(step2)
				.end()
				.build();
	}
}

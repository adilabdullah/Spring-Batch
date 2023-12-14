package com.spring.batch.config;

import java.util.*;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import com.spring.batch.entity.Customer;
import com.spring.batch.repository.CustomerRepo;

@Configuration
public class DbConfig {
	@Autowired
	CustomerRepo customerRepo;
	@Bean
	public ItemReader<Customer> DbReader()
	{
		RepositoryItemReader<Customer> reader=new RepositoryItemReader<>();
		reader.setRepository(customerRepo);
        reader.setMethodName("findAll");
        reader.setPageSize(20);

        HashMap<String, Sort.Direction> sorts = new HashMap<>();
        sorts.put("id", Sort.Direction.ASC);
        reader.setSort(sorts);

        return reader;
		


	}
	
	@Bean
	public FlatFileItemWriter<Customer> FileWriter()
	{
		FlatFileItemWriter<Customer> writer=new FlatFileItemWriter<Customer>();
		writer.setResource(new FileSystemResource("D:\\Jars\\data_csv.csv"));
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
	// create step
		@Primary
	@Bean
	public Step step2(JobRepository jobRepo1,PlatformTransactionManager transactionManager)
	{
			return new StepBuilder("step-2",jobRepo1)
					.<Customer,Customer>chunk(10,transactionManager)
					.reader(DbReader())
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

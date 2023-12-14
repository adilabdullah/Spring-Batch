package com.spring.batch.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.transaction.PlatformTransactionManager;

import com.spring.batch.entity.Customer;
import com.spring.batch.repository.CustomerRepo;

@Configuration
public class XmlDbConfig {
	@Autowired
	CustomerRepo customerRepo;
	@Bean
	public StaxEventItemReader<Customer> ReadDataFromXml()
	{
		StaxEventItemReader<Customer> reader=new StaxEventItemReader<Customer>();
		reader.setResource(new FileSystemResource("D:\\Jars\\data_xml.xml"));
		reader.setFragmentRootElementName("Customer");
		Map<String,String> aliasMap=new HashMap<>();
		aliasMap.put("Customer","com.spring.batch.entity.Customer");
		XStreamMarshaller marshaller=new XStreamMarshaller();
		marshaller.setAliases(aliasMap);
		reader.setUnmarshaller(marshaller);
		return reader;
	}
	
	@Bean
	public RepositoryItemWriter<Customer> customerWriter()
	{
		RepositoryItemWriter<Customer> repoItemWriter=new RepositoryItemWriter<>();
		repoItemWriter.setRepository(customerRepo);
		repoItemWriter.setMethodName("save");
		return repoItemWriter;
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
			return new StepBuilder("step",jobRepo1)
					.<Customer,Customer>chunk(10,transactionManager)
					.reader(ReadDataFromXml())
					.processor(customerProces())
					.writer(customerWriter())
					.build();
												
		
	}
	// create job
		@Primary
	@Bean
	public Job jobs(JobRepository jobRepository1,Step step)
	{
		return new JobBuilder("customer-jobs",jobRepository1)
				.flow(step)
				.end()
				.build();
	}
}

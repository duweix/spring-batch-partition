package com.example.spring_batch_partition;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternUtils;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

@Configuration
@EnableBatchProcessing
public class SpringBatchPartitionApp {

    public static void main(String[] args) throws Exception {
        try (ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("spring-config/application-context.xml")) {

            System.in.read();
        }
    }

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Bean
    public Job partitionerJob() {
        return jobs.get("partitionerJob").start(partitionStep()).build();
    }

    @Bean
    public TaskExecutor teskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public Step partitionStep() {
        return steps.get("partitionStep").partitioner("slaveStep", partitioner()).step(slaveStep()).gridSize(8).taskExecutor(teskExecutor()).build();
    }

    @Bean
    public CustomMultiResourcePartitioner partitioner() {
        CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
        Resource[] resources;
        try {
            resources = ResourcePatternUtils.getResourcePatternResolver(null).getResources("file:src/main/resources/input/*.csv");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> itemReader(@Value("#{stepExecutionContext[fileName]}") String filename) {
        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        String[] tokens = { "username", "userid", "transactiondate", "amount" };
        tokenizer.setNames(tokens);
        reader.setResource(new ClassPathResource("input/" + filename));
        DefaultLineMapper<Transaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new RecordFieldSetMapper());
        reader.setLinesToSkip(1);
        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    @StepScope
    public ItemWriter<Transaction> itemWriter(Marshaller marshaller, @Value("#{stepExecutionContext[opFileName]}") String filename) {
        StaxEventItemWriter<Transaction> itemWriter = new StaxEventItemWriter<>();
        itemWriter.setMarshaller(marshaller);
        itemWriter.setRootTagName("transactionRecord");
        itemWriter.setResource(new ClassPathResource("xml/" + filename));
        return itemWriter;
    }

    @Bean
    public Step slaveStep() {
        return steps.get("slaveStep").<Transaction, Transaction>chunk(1).reader(itemReader(null)).writer(itemWriter(marshaller(), null)).build();
    }

    @Bean
    public Marshaller marshaller() {
        Marshaller marshaller = new Jaxb2Marshaller();
        return marshaller;
    }

}

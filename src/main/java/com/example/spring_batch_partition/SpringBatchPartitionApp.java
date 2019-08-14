package com.example.spring_batch_partition;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ClassPathXmlApplicationContext;

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
        return jobs.get("partitionerJob")
                    .start(partitionStep())
                    .build();
    }

    @Bean
    public Step partitionStep() {
        return steps.get("partitionStep")
                    .partitioner("slaveStep", partitioner())
                    .step(slaveStep())
                    .taskExecutor(teskExecutor())
                    .build();
    }
}

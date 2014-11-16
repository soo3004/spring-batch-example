package org.exampledriven.springbatch.configuration;

import org.exampledriven.springbatch.FileReaderTasklet;
import org.exampledriven.springbatch.domain.Person;
import org.exampledriven.springbatch.PersonItemProcessor;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;

//@Configuration
@EnableBatchProcessing
public class TaskletLoopJobConfiguration {


    public static final String FILE_READER_TASKLET = "fileReaderTasklet";
    public static final String IMPORT_PERSON_STEP = "importPersonStep";

    @Bean
    @StepScope
    public FlatFileItemReader<Person> reader(ApplicationContext applicationContext) throws IOException {

        FlatFileItemReader<Person> itemReader = new FlatFileItemReader<Person>();
        itemReader.setResource(new ClassPathResource("csv/sample-data.csv"));
        itemReader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"firstName", "lastName"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});

        return itemReader;
    }

    @Bean
    public ItemProcessor<Person, Person> processor() {
        return new PersonItemProcessor();
    }

    @Bean
    public ItemWriter<Person> writer(DataSource dataSource) {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean(name = FILE_READER_TASKLET)
    public Step fileReaderTasklet(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("tasklet").tasklet(new FileReaderTasklet()).build();
    }



    @Bean
    public Job importUserJob(JobBuilderFactory jobs, @Qualifier(IMPORT_PERSON_STEP) Step importPersonStep, @Qualifier(FILE_READER_TASKLET) Step fileReaderStep) {

        FlowBuilder<Flow> flowBuilder = new FlowBuilder<Flow>("flow1");

        Flow flow = flowBuilder
            .start(fileReaderStep).on(ExitStatus.COMPLETED.getExitCode()).to(importPersonStep).next(fileReaderStep)
            .from(fileReaderStep).on(ExitStatus.EXECUTING.getExitCode()).end().build();


        return jobs.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .start(flow)
                .end()
                .build();
    }

    @Bean(name = IMPORT_PERSON_STEP)
    public Step importPersonStep(StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
                                 ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {

        return stepBuilderFactory.get("fileReaderTasklet")
                .<Person, Person> chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}

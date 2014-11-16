package org.exampledriven.springbatch.configuration;

import org.exampledriven.springbatch.LoopDecider;
import org.exampledriven.springbatch.domain.Person;
import org.exampledriven.springbatch.PersonItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class DeciderLoopJobConfiguration {


    public static final String DECIDER_IMPORT_PERSON_STEP = "deciderImportPersonStep";
    public static final String DECIDER_ITEM_READER = "deciderItemReader";
    public static final String DECIDER_ITEM_WRITER = "deciderItemWriter";
    public static final String DECIDER_ITEM_PROCESSOR = "deciderItemProcessor";

    @Bean(name = DECIDER_ITEM_READER)
    @StepScope
    public FlatFileItemReader<Person> deciderReader(ApplicationContext applicationContext) throws IOException {

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

    @Bean(name = DECIDER_ITEM_PROCESSOR)
    public ItemProcessor<Person, Person> deciderProcessor() {
        return new PersonItemProcessor();
    }

    @Bean(name = DECIDER_ITEM_WRITER)
    public ItemWriter<Person> writer(DataSource dataSource) {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource);
        return writer;
    }


    @Bean
    public Job importUserLoopJob(JobBuilderFactory jobs, @Qualifier(DECIDER_IMPORT_PERSON_STEP) Step importPersonStep) {

        LoopDecider loopDecider = new LoopDecider(10);

        FlowBuilder<Flow> flowBuilder = new FlowBuilder<Flow>("flow1");

        Flow flow = flowBuilder
            .start(importPersonStep)
            .next(loopDecider)
            .on(LoopDecider.CONTINUE)
            .to(importPersonStep)
            .from(loopDecider)
            .on(LoopDecider.COMPLETED)
            .end()
            .build();

        return jobs.get("importUserLoopJob")
                .incrementer(new RunIdIncrementer())
                .start(flow)
                .end()
                .build();
    }

    @Bean(name = DECIDER_IMPORT_PERSON_STEP)
    public Step deciderImportPersonStep(StepBuilderFactory stepBuilderFactory, @Qualifier(DECIDER_ITEM_READER) ItemReader<Person> reader,
                                 @Qualifier(DECIDER_ITEM_WRITER) ItemWriter<Person> writer, @Qualifier(DECIDER_ITEM_PROCESSOR) ItemProcessor<Person, Person> processor) {

        return stepBuilderFactory.get(DECIDER_IMPORT_PERSON_STEP)
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

package org.exampledriven.springbatch.configuration;

import javax.sql.DataSource;

import org.exampledriven.springbatch.ResourceLoggingItemWriter;
import org.exampledriven.springbatch.domain.Person;
import org.exampledriven.springbatch.PersonItemProcessor;
import org.exampledriven.springbatch.utils.ResourceUtil;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class MultiResourceJobConfiguration {

    public static final String MULTI_RESOURCE_IMPORT_PERSON_STEP = "multiResourceImportPersonStep";
    public static final String MR_ITEM_READER = "mrItemReader";
    public static final String MR_ITEM_WRITER = "mrItemWriter";
    public static final String MR_ITEM_PROCESSOR = "mrItemProcessor";

    @Bean(name = MR_ITEM_READER)
    @StepScope
    public MultiResourceItemReader<Person> mrItemReader(@Value("#{jobParameters[inputDirectory]}") String inputDirectory, ApplicationContext applicationContext) throws IOException {

        FlatFileItemReader<Person> delegateReader = new FlatFileItemReader<Person>();
        delegateReader.setResource(new ClassPathResource(inputDirectory));
        delegateReader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"firstName", "lastName"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});

        Resource[] resources = ResourceUtil.readResources(inputDirectory, "data");

        MultiResourceItemReader<Person> multiResourceItemReader = new MultiResourceItemReader<Person>();
        multiResourceItemReader.setResources(resources);
        multiResourceItemReader.setDelegate(delegateReader);

        return multiResourceItemReader;
    }

    @Bean(name = MR_ITEM_PROCESSOR)
    public ItemProcessor<Person, Person> mrProcessor() {
        return new PersonItemProcessor();
    }

    @Bean(name = MR_ITEM_WRITER)
    public ItemWriter<Person> mrWriter(DataSource dataSource) {

        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource);

        ResourceLoggingItemWriter<Person> resourceLoggingItemWriter = new ResourceLoggingItemWriter<Person>(writer);
        return resourceLoggingItemWriter;
    }

    @Bean
    public Job mrImportUserJob(JobBuilderFactory jobs, @Qualifier(MULTI_RESOURCE_IMPORT_PERSON_STEP) Step importPersonStep) {
        return jobs.get("importUserMultiResourceJob")
                .incrementer(new RunIdIncrementer())
                .flow(importPersonStep)
                .end()
                .build();
    }

    @Bean(name = MULTI_RESOURCE_IMPORT_PERSON_STEP)
    public Step mrImportPersonStep(StepBuilderFactory stepBuilderFactory, @Qualifier(MR_ITEM_READER)ItemReader<Person> reader,
                             @Qualifier(MR_ITEM_WRITER)ItemWriter<Person> writer, @Qualifier(MR_ITEM_PROCESSOR)ItemProcessor<Person, Person> processor) {
        return stepBuilderFactory.get(MULTI_RESOURCE_IMPORT_PERSON_STEP)
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

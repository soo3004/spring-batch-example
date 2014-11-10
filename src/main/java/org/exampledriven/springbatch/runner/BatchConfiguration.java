package org.exampledriven.springbatch.runner;

import javax.sql.DataSource;

import org.exampledriven.springbatch.Person;
import org.exampledriven.springbatch.PersonItemProcessor;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourceArrayPropertyEditor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {


    private Resource[] getResources(String stagingDirectory, String pattern) {
        ResourceArrayPropertyEditor resourceLoader = new ResourceArrayPropertyEditor();
        resourceLoader.setAsText("classpath:" + stagingDirectory + "/*" + pattern + "*.csv");
        Resource[] resources = (Resource[]) resourceLoader.getValue();
        return resources;
    }

    @Bean
    @StepScope
    public MultiResourceItemReader<Person> reader(@Value("#{jobParameters[inputDirectory]}") String inputDirectory, ApplicationContext applicationContext) throws IOException {

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

//        Resource[] resources = getResources(inputDirectory, "data");

        Resource[] resources = applicationContext.getResources(inputDirectory + "/*data*.csv");

        getResources(inputDirectory, "data");

        MultiResourceItemReader<Person> multiResourceItemReader = new MultiResourceItemReader<Person>();
        multiResourceItemReader.setResources(resources);
        multiResourceItemReader.setDelegate(delegateReader);

        return multiResourceItemReader;
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
    // end::readerwriterprocessor[]

    // tag::jobstep[]
    @Bean
    public Job importUserJob(JobBuilderFactory jobs, Step s1) {
        return jobs.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(s1)
                .end()
                .build();
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
            ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {
        return stepBuilderFactory.get("step1")
                .<Person, Person> chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
    // end::jobstep[]

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}

package org.exampledriven.springbatch;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.exampledriven.springbatch.domain.Person;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

@ComponentScan(basePackages = "org.exampledriven.springbatch.configuration")
@EnableAutoConfiguration
public class Application {

    public static void main(String[] args) {

        ApplicationContext ctx = SpringApplication.run(Application.class, new String[] {"inputDirectory=csv"});

        List<Person> results = ctx.getBean(JdbcTemplate.class).query("SELECT first_name, last_name FROM people", new RowMapper<Person>() {
            @Override
            public Person mapRow(ResultSet rs, int row) throws SQLException {
                return new Person(rs.getString(1), rs.getString(2), null);
            }
        });

        for (Person person : results) {
            System.out.println("Found <" + person + "> in the database.");
        }
    }

}

package org.exampledriven.springbatch;

import org.exampledriven.springbatch.domain.Person;
import org.springframework.batch.item.ItemProcessor;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

    @Override
    public Person process(final Person person) throws Exception {
        final String firstName = person.getFirstName().toUpperCase();
        final String lastName = person.getLastName().toUpperCase();

        final Person transformedPerson = new Person(firstName, lastName, person.getResource());

        System.out.println("Converting (" + person + ") into (" + transformedPerson + ")");

        return transformedPerson;
    }

}

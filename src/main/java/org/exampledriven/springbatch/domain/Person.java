package org.exampledriven.springbatch.domain;

import org.springframework.core.io.Resource;

public class Person extends BaseResourceAware {
    private String lastName;
    private String firstName;

    public Person() {
    }

    public Person(String firstName, String lastName, Resource resource) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.setResource(resource);
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        String s = "firstName: " + firstName + ", lastName: " + lastName;
        if (getResource() != null) {
            s += ", resource: " + getResource().getFilename();
        }
        return s;
    }

}

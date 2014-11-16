package org.exampledriven.springbatch.domain;

import org.springframework.batch.item.ResourceAware;
import org.springframework.core.io.Resource;

public class BaseResourceAware implements ResourceAware {

    private Resource resource;

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public Resource getResource() {
        return resource;
    }

}

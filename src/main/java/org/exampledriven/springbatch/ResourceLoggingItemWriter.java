package org.exampledriven.springbatch;

import org.exampledriven.springbatch.domain.BaseResourceAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import java.util.List;

public class ResourceLoggingItemWriter<T extends BaseResourceAware> implements ItemWriter<T>, ItemStream, InitializingBean {

    Logger logger = LoggerFactory.getLogger(ResourceLoggingItemWriter.class);

    ItemWriter < T > delegate;

    Resource currentInputResource;

    public ResourceLoggingItemWriter(ItemWriter<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        delegate.write(items);

        for (T item : items) {
            Resource resource = item.getResource();

            if (currentInputResource != null && resource != currentInputResource) {
                finishedProcessingInput(currentInputResource);
            }

            currentInputResource = resource;

        }

    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (delegate instanceof ItemStream) {
            ((ItemStream)delegate).open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (delegate instanceof ItemStream) {
            ((ItemStream)delegate).update(executionContext);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        if (delegate instanceof ItemStream) {
            ((ItemStream)delegate).close();
        }

        if (currentInputResource != null) {
            finishedProcessingInput(currentInputResource);
            currentInputResource = null;
        }


    }

    private void finishedProcessingInput(Resource resource) {
        logger.info("Finished writing all items from " + resource);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (delegate instanceof InitializingBean) {
            ((InitializingBean)delegate).afterPropertiesSet();
        }

    }
}

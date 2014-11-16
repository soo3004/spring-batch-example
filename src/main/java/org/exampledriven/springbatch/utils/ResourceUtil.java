package org.exampledriven.springbatch.utils;

import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourceArrayPropertyEditor;

public class ResourceUtil {

    public static Resource[] readResources(String stagingDirectory, String pattern) {
        ResourceArrayPropertyEditor resourceLoader = new ResourceArrayPropertyEditor();
        resourceLoader.setAsText("classpath:" + stagingDirectory + "/*" + pattern + "*.csv");
        Resource[] resources = (Resource[]) resourceLoader.getValue();
        return resources;
    }

}

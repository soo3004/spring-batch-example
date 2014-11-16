/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.exampledriven.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

/**
 * This decider will return "CONTINUE" until the limit it reached, at which
 * point it will return "COMPLETE".
 *
 * @author Dan Garrette
 * @since 2.0
 */
public class LoopDecider implements JobExecutionDecider {

    Logger logger = LoggerFactory.getLogger(LoopDecider.class);

    public static final String COMPLETED = "COMPLETED";
    public static final String CONTINUE = "CONTINUE";

    public LoopDecider(int limit) {
        this.limit = limit;
    }

    private int count = 0;

    private int limit = 1;

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        if (++count >= limit) {
            logger.info("LOOP COMPLETE");
            return new FlowExecutionStatus(COMPLETED);
        } else {
            logger.info("LOOP COUNT : " + count);
            return new FlowExecutionStatus(CONTINUE);
        }
    }

}

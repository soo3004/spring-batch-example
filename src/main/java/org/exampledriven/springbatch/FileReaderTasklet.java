package org.exampledriven.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Created by peszter on 11/15/14.
 */
public class FileReaderTasklet implements Tasklet {

    Logger logger = LoggerFactory.getLogger(FileReaderTasklet.class);

    int count = 0;
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

//        chunkContext.getStepContext().getStepExecution().set

//            return RepeatStatus.FINISHED;

//        logger.info("----------- " + count);
        if (count < 4) {
            count ++;
            contribution.setExitStatus(ExitStatus.EXECUTING);
            chunkContext.getStepContext().getStepExecution().setExitStatus(ExitStatus.EXECUTING);
            return RepeatStatus.FINISHED;

        }

        chunkContext.getStepContext().getStepExecution().setExitStatus(ExitStatus.COMPLETED);
        contribution.setExitStatus(ExitStatus.COMPLETED);
        return RepeatStatus.CONTINUABLE;

    }

}

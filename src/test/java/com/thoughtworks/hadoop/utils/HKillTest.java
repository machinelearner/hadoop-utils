package com.thoughtworks.hadoop.utils;

import org.junit.Assert;
import org.junit.Test;

public class HKillTest {
    @Test
    public void shouldNotKillAJobWhenNoArgumentGiven() {
        HCommandArgument argument = new HCommandArgument();
        String expectedNoArgumentExceptionMessage = HKill.NO_ARGUMENT_MESSAGE;
        String actualExceptionMessage = "";
        try {
            HCommandOutput commandOutput = new HKill().execute(argument);
        } catch (RuntimeException e) {
            actualExceptionMessage = e.getMessage();
        }
        Assert.assertEquals(expectedNoArgumentExceptionMessage, actualExceptionMessage);

    }

    @Test
    public void shouldNotKillAJobWhenInvalidJobIdGiven() {
        HCommandArgument argument = new HCommandArgument();
        String expectedNoJobIdMessage = HKill.INVALID_JOB_ID;
        argument.put("job", "");
        String actualExceptionMessage = "";
        try {
            HCommandOutput commandOutput = new HKill().execute(argument);
        } catch (RuntimeException e) {
            actualExceptionMessage = e.getMessage();
        }
        Assert.assertEquals(expectedNoJobIdMessage, actualExceptionMessage);
    }

    @Test
    public void shouldNotKillAJobWhenJobNotFound() {
        HCommandArgument argument = new HCommandArgument();
        String expectedNoJobIdMessage = ClusterClient.INVALID_JOB_ID;
        argument.put("job", "job_20130615_0000");
        String actualExceptionMessage = "";
        try {
            HCommandOutput commandOutput = new HKill().execute(argument);
        } catch (RuntimeException e) {
            actualExceptionMessage = e.getMessage();
        }
        Assert.assertEquals(expectedNoJobIdMessage, actualExceptionMessage);

    }

    @Test
    public void shouldKillAJob() {
        HCommandArgument argument = new HCommandArgument();
        String jobId = "job_201306121815_0023";
        argument.put("job", jobId);
        HCommandOutput commandOutput = new HKill().execute(argument);
        Assert.assertEquals(HCommandOutput.Result.SUCCESS, commandOutput.getResult());
        Assert.assertEquals(jobId, commandOutput.getOutput());
    }

}

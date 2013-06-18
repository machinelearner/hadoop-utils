package com.thoughtworks.hadoop.utils;

import org.junit.Before;
import org.junit.Test;


//More Of a Integration/Functional Test
public class HJobStatTest {

    private HCommandArgument argument;

    @Before
    public void setUp() {
        argument = new HCommandArgument();
        argument.put("u", "hadoop");
    }

    @Test
    public void shouldFetchCompletedJobDetailsFromCluster() {
        argument.put("c", "");
        HCommandOutput commandOutput = new HJobStat().execute(argument);
        System.out.println(JobDetail.formattedHeader());
        System.out.println(commandOutput.getOutput());
    }

    @Test
    public void shouldFetchRunningJobDetailsFromCluster() {
        argument.put("r", "");
        HCommandOutput commandOutput = new HJobStat().execute(argument);
        System.out.println(JobDetail.formattedHeader());
        System.out.println(commandOutput.getOutput());
    }

    @Test
    public void shouldFetchAllJobDetailsFromCluster() {
        argument.put("a", "");
        HCommandOutput commandOutput = new HJobStat().execute(argument);
        System.out.println(JobDetail.formattedHeader());
        System.out.println(commandOutput.getOutput());
    }


}

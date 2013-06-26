package com.thoughtworks.hadoop.utils;

import com.thoughtworks.hadoop.utils.commands.HCommandArgument;
import com.thoughtworks.hadoop.utils.commands.HCommandOutput;
import com.thoughtworks.hadoop.utils.commands.HJobStat;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;


//More Of a Integration/Functional Test
public class HJobStatTest {

    private HCommandArgument argument;

    ClusterClient clusterClient;

    @Before
    public void setUp() {
        argument = new HCommandArgument();
        argument.put("u", "hadoop");
        clusterClient = mock(ClusterClient.class);
    }

    @Test
    public void shouldFetchCompletedJobDetailsFromCluster() {
        argument.put("c", "");
        HCommandOutput commandOutput = new HJobStat(clusterClient).execute(argument);
        System.out.println(JobDetail.formattedHeader());
        System.out.println(commandOutput.getOutput());
        //AssertHow?
    }

    @Test
    public void shouldFetchRunningJobDetailsFromCluster() {
        argument.put("r", "");
        HCommandOutput commandOutput = new HJobStat(clusterClient).execute(argument);
        System.out.println(JobDetail.formattedHeader());
        System.out.println(commandOutput.getOutput());
    }

    @Test
    public void shouldFetchAllJobDetailsFromCluster() {
        argument.put("a", "");
        HCommandOutput commandOutput = new HJobStat(clusterClient).execute(argument);
        System.out.println(JobDetail.formattedHeader());
        System.out.println(commandOutput.getOutput());
    }


}

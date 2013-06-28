package com.thoughtworks.hadoop.utils;

import com.thoughtworks.hadoop.utils.commands.HCommandArgument;
import com.thoughtworks.hadoop.utils.commands.HCommandOutput;
import com.thoughtworks.hadoop.utils.commands.HJobStat;
import com.thoughtworks.hadoop.utils.commands.HTaskStat;
import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


//More Of a Integration/Functional Test
public class HJobStatTest {

    private HCommandArgument argument;

    ClusterClient clusterClient;

    @Before
    public void setUp() {
        argument = new HCommandArgument();
        argument.put("u", "hadoop");
        clusterClient = mock(ClusterClient.class);
        String jobName = "This is the name of the Hadoop Job";


    }

    @Test
    public void shouldFetchCompletedJobDetailsFromCluster() throws ParseException {
        argument.put("c", "");
        HCommandOutput commandOutput = new HJobStat(clusterClient).execute(argument);

        ClusterClient clusterClient = mock(ClusterClient.class);
        JobDetails jobDetails = new JobDetails();
//        jobDetails.addDetail(new JobDetail("task_201306121815_0047_m_000000", "1", "0"));
//        jobDetails.addDetail(new JobDetail("task_201306121815_0047_m_000001", "1", "1"));
        String jobId = "job_201306121815_0047";

        String fiveSpaces = "     ";
        String metadata = "JobId" + fiveSpaces + "JobName" + fiveSpaces + "User" + fiveSpaces + "Priority" +
                fiveSpaces + "MapProgress" + fiveSpaces + "ReduceProgress" + fiveSpaces + "StartTime" +
                fiveSpaces + "JobState" + "\n";
        String data_line1 = "job_201306121815_0062" + fiveSpaces +
                "INSERT OVERWRITE DIRECTORY '/user/del...DESC(Stage-1)" + fiveSpaces + "hadoop" + fiveSpaces +
                "NORMAL" + fiveSpaces + "1.0" + fiveSpaces+ "1.0" + fiveSpaces + "2013-06-28 12:13:08.061" + "SUCCEEDED" + "\n";
        String data_line2 = "job_201306121815_0063" + fiveSpaces +
                "INSERT OVERWRITE DIRECTORY '/user/del...DESC(Stage-1)" + fiveSpaces + "hadoop" + fiveSpaces +
                "NORMAL" + fiveSpaces + "1.0" + fiveSpaces+ "1.0" + fiveSpaces + "2013-06-28 12:14:08.061" + "SUCCEEDED" + "\n";

        String expectedOutput = metadata + data_line1 + data_line2;
        HCommandArgument argument = new HCommandArgument();
        when(clusterClient.getAllJobDetails()).thenReturn(jobDetails);

//        HTaskStat taskStat = new HTaskStat(clusterClient);
//        HCommandOutput output = taskStat.execute(argument);
//        String actualOutput = output.getOutput();
//        assertEquals(expectedOutput, actualOutput);
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

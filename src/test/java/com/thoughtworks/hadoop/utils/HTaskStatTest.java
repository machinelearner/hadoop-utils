package com.thoughtworks.hadoop.utils;

import com.thoughtworks.hadoop.utils.commands.HCommandArgument;
import com.thoughtworks.hadoop.utils.commands.HCommandOutput;
import com.thoughtworks.hadoop.utils.commands.HTaskStat;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HTaskStatTest {

    @Test
    public void shouldGiveListOfTasksForAJob() throws ParseException {
        ClusterClient clusterClient = mock(ClusterClient.class);
        TaskDetails taskDetails = new TaskDetails();
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000000", "1", "0"));
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000001", "1", "1"));
        String jobId = "job_201306121815_0047";

        String fiveSpaces = "     ";
        String metadata = "Task_id" + fiveSpaces + "Times failed" + fiveSpaces + "Times Killed" + "\n";
        String data_line1 = "task_201306121815_0047_m_000000" + fiveSpaces + "1" + fiveSpaces + "0" + "\n";
        String data_line2 = "task_201306121815_0047_m_000001" + fiveSpaces + "1" + fiveSpaces + "1" + "\n";
        String expectedOutput = metadata + data_line1 + data_line2;
        HCommandArgument argument = new HCommandArgument();
        argument.put("jid", jobId);
        when(clusterClient.getAllTaskDetails(jobId)).thenReturn(taskDetails);

        HTaskStat taskStat = new HTaskStat(clusterClient);
        HCommandOutput output = taskStat.execute(argument);
        String actualOutput = output.getOutput();
        assertEquals(expectedOutput, actualOutput);

    }

    @Test
    public void shouldGiveListOfMapTasksForAJob() throws ParseException {
        ClusterClient clusterClient = mock(ClusterClient.class);
        TaskDetails taskDetails = new TaskDetails();
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000000", "1", "0"));
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000001", "1", "1"));
        String jobId = "job_201306121815_0047";

        String fiveSpaces = "     ";
        String metadata = "Task_id" + fiveSpaces + "Times failed" + fiveSpaces + "Times Killed" + "\n";
        String data_line1 = "task_201306121815_0047_m_000000" + fiveSpaces + "1" + fiveSpaces + "0" + "\n";
        String data_line2 = "task_201306121815_0047_m_000001" + fiveSpaces + "1" + fiveSpaces + "1" + "\n";
        String expectedOutput = metadata + data_line1 + data_line2;
        HCommandArgument argument = new HCommandArgument();
        argument.put("jid", jobId);
        argument.put("m", "");
        when(clusterClient.getMapTaskDetails(jobId)).thenReturn(taskDetails);

        HTaskStat taskStat = new HTaskStat(clusterClient);
        HCommandOutput output = taskStat.execute(argument);
        String actualOutput = output.getOutput();
        assertEquals(expectedOutput, actualOutput);

    }

    @Test
    public void shouldGetEmptyReduceTasksForAJobWhenItDoesNotExist() throws ParseException {
        ClusterClient clusterClient = mock(ClusterClient.class);
        TaskDetails taskDetails = new TaskDetails();
        String jobId = "job_201306121815_0047";

        String fiveSpaces = "     ";
        String expectedOutput = "Task_id" + fiveSpaces + "Times failed" + fiveSpaces + "Times Killed" + "\n";
        HCommandArgument argument = new HCommandArgument();
        argument.put("jid", jobId);
        argument.put("m", "");
        when(clusterClient.getMapTaskDetails(jobId)).thenReturn(taskDetails);

        HTaskStat taskStat = new HTaskStat(clusterClient);
        HCommandOutput output = taskStat.execute(argument);
        String actualOutput = output.getOutput();
        assertEquals(expectedOutput, actualOutput);

    }

    @Test
    public void shouldGiveListOfReduceTasksForAJob() throws ParseException {
        ClusterClient clusterClient = mock(ClusterClient.class);
        TaskDetails taskDetails = new TaskDetails();
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000000", "1", "0"));
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000001", "1", "1"));
        String jobId = "job_201306121815_0047";

        String fiveSpaces = "     ";
        String metadata = "Task_id" + fiveSpaces + "Times failed" + fiveSpaces + "Times Killed" + "\n";
        String data_line1 = "task_201306121815_0047_m_000000" + fiveSpaces + "1" + fiveSpaces + "0" + "\n";
        String data_line2 = "task_201306121815_0047_m_000001" + fiveSpaces + "1" + fiveSpaces + "1" + "\n";
        String expectedOutput = metadata + data_line1 + data_line2;
        HCommandArgument argument = new HCommandArgument();
        argument.put("jid", jobId);
        argument.put("r", "");
        when(clusterClient.getReduceTaskDetails(jobId)).thenReturn(taskDetails);

        HTaskStat taskStat = new HTaskStat(clusterClient);
        HCommandOutput output = taskStat.execute(argument);
        String actualOutput = output.getOutput();
        assertEquals(expectedOutput, actualOutput);

    }


}

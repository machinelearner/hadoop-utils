package com.thoughtworks.hadoop.utils;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HTaskStatTest {

    @Test
    public void shouldGiveListOfTasksForAJob() {
        ClusterClient clusterClient = mock(ClusterClient.class);
        TaskDetails taskDetails = new TaskDetails();
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000000", "1", "0"));
        taskDetails.addDetail(new TaskDetail("task_201306121815_0047_m_000001", "1", "1"));
        when(clusterClient.getTaskDetails()).thenReturn(taskDetails);
        HTaskStat taskStat = new HTaskStat(clusterClient);
        HCommandOutput output = taskStat.execute(new HCommandArgument());
        String actualOutput = output.getOutput();
        String fiveSpaces = "     ";
        String metadata = "Task_id" + fiveSpaces + "Times failed" + fiveSpaces + "Times Killed" + "\n";
        String data_line1 = "task_201306121815_0047_m_000000" + fiveSpaces + "1" + fiveSpaces + "0" + "\n";
        String data_line2 = "task_201306121815_0047_m_000001" + fiveSpaces + "1" + fiveSpaces + "1" + "\n";
        String expectedOutput = metadata + data_line1 + data_line2;
        assertEquals(expectedOutput, actualOutput);

    }
}

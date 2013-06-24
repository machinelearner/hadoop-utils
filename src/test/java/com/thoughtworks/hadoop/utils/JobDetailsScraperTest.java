package com.thoughtworks.hadoop.utils;

import org.jsoup.Jsoup;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobDetailsScraperTest {
    @Test
    public void shouldGetTaskDetailsForAJob() throws IOException {
        String killedFileURL = "./src/test/html/failed_task.html";
        String failedFileURL = "./src/test/html/failed_task.html";
        TaskDetail task_m_000003 = new TaskDetail("task_201306121815_0047_m_000003", "1", "1");
        TaskDetails expectedTaskDetail = new TaskDetails();
        expectedTaskDetail.addDetail(task_m_000003);

        FileSource fileSource = mock(FileSource.class);
        when(fileSource.get("killed")).thenReturn(Jsoup.parse(new File(failedFileURL), "UTF-8"));
        when(fileSource.get("failed")).thenReturn(Jsoup.parse(new File(killedFileURL), "UTF-8"));
        JobDetailsScraper jobDetailsScraper = new JobDetailsScraper(fileSource);
        TaskDetails actualTaskDetail = jobDetailsScraper.getTaskDetails();
        Assert.assertEquals(expectedTaskDetail, actualTaskDetail);
    }

}

package com.thoughtworks.hadoop.utils;

import org.junit.Test;

public class JobDetailsScraperTest {
    @Test
    public void shouldGetNumberOfMapTasksForAJob() {
        String jobTrackerUrl = "http://hadoop03.corporate.thoughtworks.com:50030";
        JobDetailsScraper jobDetailsScraper = new JobDetailsScraper(jobTrackerUrl);
        String jobId = "job_201306121815_0036";
        int numberOfMapTasks = jobDetailsScraper.getNumberOfMapTasks(jobId);


    }
}

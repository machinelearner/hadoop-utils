package com.thoughtworks.hadoop.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;

public class JobDetailsScraper {
    private String jobTrackerURL;
    public static final String JOB_DETAIL_URL_TEMPLATE = "%s/jobdetails.jsp?jobid=%s";

    public JobDetailsScraper(String jobTrackerURL) {
        this.jobTrackerURL = jobTrackerURL;
    }

    public int getNumberOfMapTasks(String jobId) {
        String jobDetailsURL = getJobDetailsURL(jobId);
        Document jobDetailsPage;
        try {
            jobDetailsPage = Jsoup.connect(jobDetailsURL).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Element jobTable = jobDetailsPage.select("table").get(0);
        Element mapRow = jobTable.getElementsByTag("tr").get(1);
        Element cells = mapRow.getElementsByTag("td").get(5);
        System.out.println("cells = " + cells);
        return 0;
    }

    public String getJobDetailsURL(String jobId) {
        return String.format(JOB_DETAIL_URL_TEMPLATE, jobTrackerURL, jobId);
    }
}

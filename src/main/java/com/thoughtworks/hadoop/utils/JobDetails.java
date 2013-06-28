package com.thoughtworks.hadoop.utils;

import java.util.ArrayList;

public class JobDetails {

    private ArrayList<JobDetail> jobDetails = new ArrayList<JobDetail>();

    public void addDetail(JobDetail jobDetail) {
        this.jobDetails.add(jobDetail);
    }

}

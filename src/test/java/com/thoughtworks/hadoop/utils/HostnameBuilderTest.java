package com.thoughtworks.hadoop.utils;


import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HostnameBuilderTest {

    @Test
    public void shouldReturnDefaultHostnameForInvalidTasktrackerName() {
        String localhost_tracker_name = "tracker_//localhost:12345";
        String extractedHostname = HostnameBuilder.extractHostNameFromTaskTrackerName(localhost_tracker_name);

        assertEquals(HostnameBuilder.INVALID_HOSTNAME, extractedHostname);
    }

    @Test
    public void shouldReturnDefaultHostnameForInvalidTasktrackerNameWithSpecialCharsMixed() {
        String localhost_tracker_name = "tracker_loca-lh+o%st:12345";
        String extractedHostname = HostnameBuilder.extractHostNameFromTaskTrackerName(localhost_tracker_name);

        assertEquals(HostnameBuilder.INVALID_HOSTNAME, extractedHostname);
    }

    @Test
    public void shouldExtractHostNameGivenTaskTrackerNameAsLocalhost() {
        String localhost_tracker_name = "tracker_localhost:localhost:12345";
        String extractedHostname = HostnameBuilder.extractHostNameFromTaskTrackerName(localhost_tracker_name);

        assertEquals("localhost", extractedHostname);
    }

    @Test
    public void shouldExtractHostNameGivenTaskTrackerNameAsAHostname() {
        String localhost_tracker_name = "tracker_jobtracker.hadoopcluster.com:localhost://localhost:12345";
        String extractedHostname = HostnameBuilder.extractHostNameFromTaskTrackerName(localhost_tracker_name);

        assertEquals("jobtracker.hadoopcluster.com", extractedHostname);
    }

    @Test
    public void shouldExtractHostNameGivenTaskTrackerNameAsAHostnameWithUnderscore() {
        String localhost_tracker_name = "tracker__jobtracker.hadoopcluster.com:localhost://localhost:12345";
        String extractedHostname = HostnameBuilder.extractHostNameFromTaskTrackerName(localhost_tracker_name);

        assertEquals("_jobtracker.hadoopcluster.com", extractedHostname);
    }




}

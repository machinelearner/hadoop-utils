package com.thoughtworks.hadoop.utils;


import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HMountTest {

//
//    @Test
//    public void shouldNotFailToConnectToJobTracker() throws IOException {
//        HMount hMount = new HMount("hadoop01.utils.com", 23456);
//        JobClient jobClient = mock(JobClient.class);
//        // need to mock InetAddress and what not
//        Collection<String> listOfTaskTracker = new ArrayList<String>();
//        listOfTaskTracker.add("hadoop01.utils.com:23456");
//        listOfTaskTracker.add("hadoop02.utils.com:12345");
//        ClusterStatus clusterStatus = mock(ClusterStatus.class);
//        when(jobClient.getClusterStatus(true)).thenReturn(clusterStatus);
//        when(clusterStatus.getActiveTrackerNames()).thenReturn(listOfTaskTracker);
//
//        TestCase.assertEquals(listOfTaskTracker.size(), hMount.getTaskTrackerNames().size());
//
//    }

    @Test
    public void shouldGetListOfTaskTrackers() throws IOException {
        ClusterClient mockClient = mock(ClusterClient.class);
        List<String> actualTaskTrackerNames = Arrays.asList("tracker_hadoop01.com:bla.bla", "tracker_hadoop02.com:localhost:localhost");
        List<String> expectedTaskTrackerNames = Arrays.asList("hadoop01.com", "hadoop02.com");
        when(mockClient.getTaskTrackerNames()).thenReturn(actualTaskTrackerNames);
        HMount hMount = new HMount(mockClient);
        Collection<String> taskTrackerNames = hMount.getTaskTrackerNames();
        verify(mockClient).getTaskTrackerNames();
        Assert.assertFalse("Should not be empty", taskTrackerNames.isEmpty());
        Assert.assertEquals(expectedTaskTrackerNames, taskTrackerNames);
    }


    //do the right thing with the test cases!!

}

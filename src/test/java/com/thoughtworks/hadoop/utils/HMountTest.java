package com.thoughtworks.hadoop.utils;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class HMountTest {
    private ClusterClient mockClient;
    private HMount hMount;
    private HCommandArgument argument;

    @Before
    public void setUp() throws Exception {
        mockClient = mock(ClusterClient.class);
        hMount = new HMount(mockClient);
        argument = new HCommandArgument();
        argument.put("f", "./hadoop");
    }

    @After
    public void tearDown() throws Exception {
        String dir_name = argument.get("f");
        File hadoopDir = new File(dir_name);
        FileUtils.deleteDirectory(hadoopDir);

    }

    @Test
    public void shouldGetListOfTaskTrackers() throws IOException {
        List<String> actualTaskTrackerNames = Arrays.asList("tracker_hadoop01.com:bla.bla", "tracker_hadoop02.com:localhost:localhost");
        List<String> expectedTaskTrackerNames = Arrays.asList("hadoop01.com", "hadoop02.com");

        when(mockClient.getTaskTrackerNames()).thenReturn(actualTaskTrackerNames);
        Collection<String> taskTrackerNames = hMount.getTaskTrackerNames();
        verify(mockClient).getTaskTrackerNames();
        Assert.assertFalse("Should not be empty", taskTrackerNames.isEmpty());
        Assert.assertEquals(expectedTaskTrackerNames, taskTrackerNames);
    }

    @Test
    public void shouldExecuteCommandSuccessfully() {
        HCommandOutput output = hMount.execute(argument);
        assertEquals(HCommandOutput.Result.SUCCESS, output.getResult());
    }

    @Test
    public void shouldExecuteCommandAndGetClusterDetails() {
        List<String> expectedTaskTrackerNames = Arrays.asList("hadoop01.com", "hadoop02.com");
        List<String> actualTaskTrackerNames = Arrays.asList("tracker_hadoop01.com:bla.bla", "tracker_hadoop02.com:localhost:localhost");
        String jobTrackerName = "jobtracker.hadoop.com";

        when(mockClient.getTaskTrackerNames()).thenReturn(actualTaskTrackerNames);
        when(mockClient.getJobTrackerName()).thenReturn(jobTrackerName);
        HCommandOutput output = hMount.execute(argument);


        assertEquals(HCommandOutput.Result.SUCCESS, output.getResult());
        Gson gson = new Gson();
        JsonObject expectedJSON = new JsonObject();
        String taskTrackers = gson.toJson(expectedTaskTrackerNames);
        expectedJSON.addProperty("taskTrackers", taskTrackers);
        expectedJSON.addProperty("jobTracker", jobTrackerName);
        String expected = expectedJSON.toString();
        assertEquals(expected, output.getOutput());


    }

}

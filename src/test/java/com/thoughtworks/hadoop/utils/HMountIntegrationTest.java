package com.thoughtworks.hadoop.utils;

import org.junit.Test;

public class HMountIntegrationTest {

    @Test
    public void shouldRunAgainstTheCluster() {
        // Following line is required to run the client remotely as Super User - in this case running as hadoop
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        HMount hMount = new HMount(new ClusterClient("hadoop03.corporate.thoughtworks.com", 23456,"hadoop02.corporate.thoughtworks.com",12345));
        hMount.execute(new HCommandArgument());
    }
}

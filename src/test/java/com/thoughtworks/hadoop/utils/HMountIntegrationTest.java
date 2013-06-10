package com.thoughtworks.hadoop.utils;

import org.junit.Test;

public class HMountIntegrationTest {

    @Test
    public void shouldRunAgainstTheCluster() {
        HMount hMount = new HMount(new ClusterClient("hadoop03.corporate.thoughtworks.com", 23456));
        hMount.execute(new HCommandArgument());
    }
}

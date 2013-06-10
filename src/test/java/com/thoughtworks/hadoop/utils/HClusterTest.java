package com.thoughtworks.hadoop.utils;

import org.junit.Test;

public class HClusterTest {
    @Test
    public void shouldRevealClusterDetails() {
        HCluster cluster = new HCluster();
        cluster.execute(new HCommandArgument());
    }
}

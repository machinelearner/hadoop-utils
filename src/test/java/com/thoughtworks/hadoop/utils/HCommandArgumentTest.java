package com.thoughtworks.hadoop.utils;

import junit.framework.Assert;
import org.junit.Test;

public class HCommandArgumentTest {

    @Test
    public void shouldBuildArgumentsFromCommandLineArgs() {
        String[] commandLineArgs = {"someCommand", "-j", "jobtracker.hadoop.com", "-p", "12345"};
        HCommandArgument argument = HCommandArgument.create(commandLineArgs, HMount.options());
        Assert.assertEquals(argument.get("j"), "jobtracker.hadoop.com");

    }
}

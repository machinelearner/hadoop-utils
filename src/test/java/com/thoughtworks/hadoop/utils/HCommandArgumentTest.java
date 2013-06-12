package com.thoughtworks.hadoop.utils;

import junit.framework.Assert;
import org.junit.Test;

public class HCommandArgumentTest {

    @Test
    public void shouldBuildArgumentsFromCommandLineArgs() {
        String[] commandLineArgs = {"someCommand", "-j", "jobtracker.hadoop.com", "-jp", "12345"};
        HCommandArgument argument = HCommandArgument.create(commandLineArgs, HMount.options());
        Assert.assertEquals(argument.get("j"), "jobtracker.hadoop.com");
        Assert.assertEquals(argument.get("jp"), "12345");

    }

    @Test
    public void shouldBuildArgumentsFromCommandLineArgsWithJTAndNN() {
        String[] commandLineArgs = {"someCommand", "-j", "jobtracker.hadoop.com", "-jp", "12345","-n","namenode.hadoop.com","-np","23456"};
        HCommandArgument argument = HCommandArgument.create(commandLineArgs, HMount.options());
        Assert.assertEquals(argument.get("j"), "jobtracker.hadoop.com");
        Assert.assertEquals(argument.get("jp"), "12345");
        Assert.assertEquals(argument.get("n"), "namenode.hadoop.com");
        Assert.assertEquals(argument.get("np"), "23456");
    }
}

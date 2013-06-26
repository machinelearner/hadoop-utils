package com.thoughtworks.hadoop.utils;

import com.thoughtworks.hadoop.utils.commands.HMount;
import org.junit.Test;

import java.io.IOException;

public class HShellTest {

    @Test
    public void shouldLaunchTheShell() throws IOException {
        HShell hShell = new HShell();
        hShell.start();
        HMount command = new HMount(null);
        hShell.execute(command);
    }
}

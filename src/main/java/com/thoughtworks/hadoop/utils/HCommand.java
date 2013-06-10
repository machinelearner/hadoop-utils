package com.thoughtworks.hadoop.utils;

public interface HCommand {
    boolean wasSuccessful();

    HCommandOutput execute(HCommandArgument hCommandArgument);
}

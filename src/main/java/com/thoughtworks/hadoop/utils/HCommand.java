package com.thoughtworks.hadoop.utils;

public interface HCommand {

    HCommandOutput execute(HCommandArgument hCommandArgument);
}

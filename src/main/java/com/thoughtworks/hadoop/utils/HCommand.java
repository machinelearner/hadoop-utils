package com.thoughtworks.hadoop.utils;

public interface HCommand {
    HCommandArgument applyDefaults(HCommandArgument hCommandArgument);

    HCommandOutput execute(HCommandArgument hCommandArgument);

}

package com.thoughtworks.hadoop.utils.commands;

import org.apache.commons.cli.ParseException;

public interface HCommand {
    HCommandArgument applyDefaults(HCommandArgument hCommandArgument);

    HCommandOutput execute(HCommandArgument hCommandArgument) throws ParseException;

}

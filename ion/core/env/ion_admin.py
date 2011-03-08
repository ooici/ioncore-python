#!/usr/bin/env python
import sys
from ion.core.env import args, commands
def main():
    args.parse_and_run_command(sys.argv[1:], commands, default_command="help")

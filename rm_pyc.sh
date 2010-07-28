#!/bin/bash
#
# A utility file to remove rogue 'pyc' files after any operation that moves
# or renames files
#


find . -name '*.pyc' -type f -print0 | xargs -0 /bin/rm -f
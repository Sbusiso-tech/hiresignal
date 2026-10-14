#!/bin/sh
# Docker creates the mounted upload volume owned by root; hand it to the partner account.
chown 1001:100 /home/*/upload

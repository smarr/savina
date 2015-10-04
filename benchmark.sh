#!/bin/sh
rebench -c -d --scheduler=random --without-nice rebench.conf

REV=`git rev-parse HEAD | cut -c1-8`
TARGET_PATH=~/benchmark-results/savina/$REV
mkdir -p $TARGET_PATH
cp savina.data $TARGET_PATH/

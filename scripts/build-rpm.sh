#!/bin/bash

NAME="python-pika"
VERSION="0.9.13"
VERSION="0"

tar -cvzf $HOME/rpmbuild/SOURCES/${NAME}-${VERSION}-${RELEASE}.tar.gz * \
--exclude .git


rpmbuild -ba ${NAME}.spec


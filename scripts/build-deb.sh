#!/bin/bash

NAME="python-pika"
TMP_DIR=/tmp/python-pika

cd `dirname $0`
cd ..

# Set version information
. scripts/build-pkg-functions
# set_version $1 $2
# generate_version_file $VERSION $RELEASE

# Build package
DIST_DIR=${TMP_DIR}/dist
python setup.py sdist --dist-dir=${DIST_DIR}
mv ${DIST_DIR}/$NAME-$VERSION-$RELEASE.tar.gz $NAME\_$VERSION.$RELEASE.orig.tar.gz
dpkg-buildpackage -i -I -rfakeroot

# Clean-up
python setup.py clean
make -f debian/rules clean
find . -name '*.pyc' -delete
rm -rf *.egg-info
rm -f $NAME\_$VERSION.$RELEASE.orig.tar.gz

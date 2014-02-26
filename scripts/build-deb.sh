#!/bin/bash

export DEB_BUILD_OPTIONS=nocheck
NAME="python-pika"
TMP_DIR=/tmp/python-pika
DEBEMAIL="builder-deb@comodit.com"
DEBFULLNAME="ComodIT"

cd `dirname $0`
cd ..

# Set version information
VERSION=`git describe --tags --long | awk -F"-" '{print $1}'`
RELEASE=`git describe --tags --long | awk -F"-" '{print $2}'`
COMMIT=`git describe --tags --long | awk -F"-" '{print $3}'`
MESSAGE="Release $VERSION-$RELEASE-$COMMIT"

echo $MESSAGE

export DEBEMAIL
export DEBFULLNAME

debchange --newversion $VERSION-$RELEASE "$MESSAGE"

unset DEBEMAIL
unset DEBFULLNAME

# Move Makefile (integration tests file)
mv Makefile Makefile.build-deb

# Build package
DIST_DIR=${TMP_DIR}/dist
python setup.py sdist --dist-dir=${DIST_DIR}
mv ${DIST_DIR}/*.tar.gz ../$NAME\_$VERSION.orig.tar.gz
dpkg-buildpackage -i -I -rfakeroot

# Clean-up
python setup.py clean
make -f debian/rules clean
find . -name '*.pyc' -delete
rm -rf *.egg-info
rm -f $NAME\_$VERSION.$RELEASE.orig.tar.gz

# Move Makefile (integration tests file)
mv Makefile.build-deb Makefile

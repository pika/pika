#!/bin/bash

NAME="python-pika"
SPEC_FILE_NAME="python-pika"
PLATFORMS="epel-6-i386 fedora-18-i386 fedora-19-i386 fedora-20-i386"
TAR_CONTENT="pika setup.py docs examples integration.cfg LICENSE-GPL-2.0 LICENSE-MPL-Pika README.md tests utils"

cd `dirname $0`
cd ..

# Set version information
. scripts/build-pkg-functions
set_version $1 $2

tar -cvzf $HOME/rpmbuild/SOURCES/${NAME}-${VERSION}-${RELEASE}.tar.gz ${TAR_CONTENT}

sed "s/#VERSION#/${VERSION}/g" ${SPEC_FILE_NAME}.spec.template > ${SPEC_FILE_NAME}.spec
sed -i "s/#RELEASE#/${RELEASE}/g" ${SPEC_FILE_NAME}.spec
sed -i "s/#COMMIT#/${COMMIT}/g" ${SPEC_FILE_NAME}.spec
rpmbuild -ba ${NAME}.spec

for platform in $PLATFORMS
do
    /usr/bin/mock -r ${platform} --rebuild $HOME/rpmbuild/SRPMS/${NAME}-${VERSION}-${RELEASE}*.src.rpm
done

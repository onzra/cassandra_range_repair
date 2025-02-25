#!/bin/bash

# build an sdist so we can allow pbr to calculate the package version
python setup.py sdist

# get package name from Makefile
PACKAGE=$(grep "PACKAGE.*:=" Makefile | sed -e "s/[ \t]*//g" -e "s/.*:=//")

# parse the package version
VERSION_BUILDER=$(ls dist/$PACKAGE-*.tar.gz)
VERSION_BUILDER=${VERSION_BUILDER#*$PACKAGE-}
VERSION_BUILDER=${VERSION_BUILDER%.tar.gz}
VERSION=${VERSION_BUILDER}

cp debian/changelog debian/changelog.save

# update the changelog file with the package version
dch -v $VERSION -m auto-build

dpkg-buildpackage -b -us -uc

mv debian/changelog.save debian/changelog

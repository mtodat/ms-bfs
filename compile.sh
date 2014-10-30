#!/bin/bash
HOMEDIR=$PWD/$(dirname $0) #this is actually src dir
echo "Extracting boost..."
tar xzf ${HOMEDIR}/vida/lib/boost.tar.gz -C ${HOMEDIR}/.
cd ${HOMEDIR}/boost
echo "Bootstrapping..."
./bootstrap.sh --with-libraries=chrono,filesystem,iostreams,system,thread,timer
echo "Compiling boost..."
./b2 -s"NO_BZIP2=1" -j8 threading=multi variant=release link=static
cd ${HOMEDIR}
make -j8 

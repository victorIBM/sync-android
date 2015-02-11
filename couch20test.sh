#!/bin/bash
PATH=$PATH:/usr/local/bin/

#get the docker env info
$(boot2docker shellinit)


#need to clone couchdb first...
git clone git@github.com:apache/couchdb.git
if [ "$?" != "0" ]; then exit $?; fi
#now clone my gist so docker can work, will need to contribute back when Jessie becomes stable
git clone git@gist.github.com:/63fef01b95afe3c4095c.git
if [ "$?" != "0" ]; then exit $?; fi


cd couchdb
rm Dockerfile #remove default docker file currently doesn't work
cp ../63fef01b95afe3c4095c/Dockerfile ./Dockerfile

docker build -t "couchdb-master" .

if [ "$?" != "0" ]; then exit $?; fi

cd ..

container=$(docker run -p 15984:15984 -d couchdb-master)

if [ "$?" != "0" ]; then exit $?; fi

#tests can be run against couch 2-0 when its done
./gradlew clean check integrationTest -Dtest.couch.host=$(boot2docker ip) -Dtest.with.specified.couch=true -Dtest.couch.port=15984 #will need to capture exit code

build=$?

#then you tear down everything
docker stop $container
docker rm $container
docker rmi "couchdb-master"

#delete clone
rm -rf couchdb
rm -rf 63fef01b95afe3c4095c
exit $build
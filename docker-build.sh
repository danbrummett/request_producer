#! /bin/sh
BINNAME="request_producer"
BUILDCONTAINER="build_request_producer"
APPCONTAINER="dbrummett/request_producer"
APPVERSION="latest"
docker build -f build_Dockerfile "$PWD" -t $BUILDCONTAINER
docker run --rm -v "$PWD":/$BINNAME $BUILDCONTAINER cp /app/$BINNAME /$BINNAME
docker build "$PWD" -t $APPCONTAINER:$APPVERSION

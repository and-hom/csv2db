#!/bin/sh
# It's not the best way to build golang source package, but I don't know another
GOPATH=$(pwd)/obj-x86_64-linux-gnu/ go get github.com/and-hom/csv2db
debuild -S
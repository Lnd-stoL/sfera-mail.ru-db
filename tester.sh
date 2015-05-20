#!/bin/bash


function update_lib {
	cp ./sfera-db/bin/lib/libsfera_db.so ./runner/libmydb.so 
}


function rebuild {
	cd sfera-db
	make $1
	cd ..
	update_lib
}


if [ "$1" == "rebuild" ]; then
	rebuild	$2
else
	echo "error: invalid command"
fi


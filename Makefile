	

update:
	cp ./sfera-db/bin/lib/libsfera_db.so ./runner/libmydb.so


build_update:
	cd ./sfera-db/; make lib
	make update


build_update_dbg:
	cd ./sfera-db
	make lib_dbg
	cd ..
	make update


test_rwd:
	make build_update
	cd ./runner; make test_rwd


test_rwd_dbg:
	make build_update_dbg
	cd ./runner; gdb ./test_speed

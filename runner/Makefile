all:
	g++ main.cpp database.cpp -lyaml-cpp -ldl -o test_speed --std=c++11 -lpthread -O2\
		-I/usr/local/Cellar/yaml-cpp/0.5.1/include/ \
		-I/usr/local/Cellar/boost/1.56.0/include/ \
		-L/usr/local/Cellar/yaml-cpp/0.5.1/lib/ \
		-L/usr/local/Cellar/boost/1.56.0/lib/

test_uni:
	rm -rf ./mydbpath
	./test_speed ../workloads/workload.uni
	diff ../workloads/workload.uni.out ../workloads/workload.uni.out.yours >> diffs
	cat ./diffs

test_old:
	rm -rf ./mydbpath
	./test_speed ../workloads/workload.old
	diff ../workloads/workload.old.out ../workloads/workload.old.out.yours >> diffs
	cat ./diffs

test_lat:
	rm -rf ./mydbpath
	./test_speed ../workloads/workload.lat
	diff ../workloads/workload.lat.out ../workloads/workload.lat.out.yours >> diffs
	cat ./diffs

test_rwd:
	rm -rf ./mydbpath
	./test_speed ../workloads/rwd-workloads
	diff ../workloads/rwd-workloads.out ../workloads/rwd-workloads.out.yours >> diffs
	cat ./diffs

test_rwd_silent:
	rm -rf ./mydbpath
	./test_speed ../workloads/rwd-workloads ./libmydb.so --silent
	diff ../workloads/rwd-workloads.out ../workloads/rwd-workloads.out.yours >> diffs
	cat ./diffs

test_rwd_big:
	rm -rf ./mydbpath
	./test_speed ../workloads/rwd-big

test_sophia_rwd:
	rm -rf ./mydbpath
	./test_speed ../workloads/rwd-workloads ../sophia/libdbsophia.so

test_all:
	./test.sh


#/bin/bash

echo "generating workload..."
cd ../gen_workload
python ./gen_workload.py --config ./example.schema.yml
cp ./output.in ../workloads/test.in
cd ../runner

echo "running sophia"
rm -rf ./mydbpath
./test_speed ../workloads/test ../sophia/libdbsophia.so
cp ../workloads/test.out.yours ../workloads/test.out

echo "running sfera-db"
rm -rf ./mydbpath
./test_speed ../workloads/test

 
echo "cat diff"
diff ../workloads/test.out ../workloads/test.out.yours


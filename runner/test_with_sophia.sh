#!/bin/bash


RED='\033[0;31m'       #  ${RED}      # красный цвет знаков
GREEN='\033[0;32m'     #  ${GREEN}    # зелёный цвет знаков
MAGENTA='\033[0;36m'   #  ${MAGENTA}    # фиолетовый цвет знаков
NORMAL='\033[0m'       #  ${NORMAL}    # все атрибуты по умолчанию

#--------------------------------------------------------------------------------------------------

REPETITIONS=${1:-1}

echo -e "${MAGENTA} generating workload... ${NORMAL}"
cd ../gen_workload
python ./gen_workload.py --config ./example.schema.yml
rm ./output.in.big
for ((n=0;n<REPETITIONS;n++)); do cat output.in >> output.in.big; done
echo -e "${MAGENTA} generated: " $(wc -l ./output.in.big)
cp ./output.in.big ../workloads/test.in
cd ../runner

echo -e "${MAGENTA} running sophia ... ${NORMAL}"
rm -rf ./mydbpath
./test_speed ../workloads/test ../sophia/libdbsophia.so
cp ../workloads/test.out.yours ../workloads/test.out

echo -e "${MAGENTA} running sfera-db ... ${NORMAL}"
rm -rf ./mydbpath
./test_speed 2>stderr.txt ../workloads/test
 
echo -e "${MAGENTA} difference test: ${NORMAL}"
diff -q ../workloads/test.out ../workloads/test.out.yours > ./diff_erorrs.txt

status=$?
if [ $status -eq 0 ] 
then 
	echo -e "${GREEN} [OK] ${NORMAL}"
else 
	echo -e "${RED} [FAILED] ${NORMAL}" 
fi

echo -n " "


BUILDDIR ?= build

HOST_BINARY=${BUILDDIR}/host_app
HOST_SOURCES=$(wildcard host/*.c)
HOST_HEADERS=$(wildcard host/*.h)

DPU_BINARY=${BUILDDIR}/dpu_task
DPU_SOURCES=$(wildcard dpu/src/*.c)
DPU_HEADERS=$(wildcard dpu/inc/*.h)

COMMONS_HEADERS=$(wildcard common/inc/*.h)

OUTPUT_FILE=${BUILDDIR}/output.txt
PLOTDATA_FILE=${BUILDDIR}/plotdata.csv

CHECK_FORMAT_FILES=${HOST_SOURCES} ${HOST_HEADERS} ${DPU_SOURCES} ${DPU_HEADERS} ${COMMONS_HEADERS}
CHECK_FORMAT_DEPENDENCIES=$(addsuffix -check-format,${CHECK_FORMAT_FILES})

NR_TASKLETS ?= 16

__dirs := $(shell mkdir -p ${BUILDDIR})

.PHONY: all clean run plotdata check check-format

all: ${HOST_BINARY} ${DPU_BINARY}
clean:
	rm -rf ${BUILDDIR}

###
### HOST APPLICATION
###
CFLAGS=-g -Wall -Werror -Wextra -O0 -std=c11 `dpu-pkg-config --cflags dpu` -Ihost/inc -Icommon/inc -DNR_TASKLETS=${NR_TASKLETS}
LDFLAGS=`dpu-pkg-config --libs dpu` -fopenmp

${HOST_BINARY}: ${HOST_SOURCES} ${HOST_HEADERS} ${COMMONS_HEADERS} ${DPU_BINARY}
	$(CC) -o $@ ${HOST_SOURCES} $(LDFLAGS) $(CFLAGS) -DDPU_BINARY=\"$(realpath ${DPU_BINARY})\"

###
### DPU BINARY
###
DPU_FLAGS=-g -O2 -Wall -Werror -Wextra -flto=thin -Idpu/inc -Icommon/inc -DNR_TASKLETS=${NR_TASKLETS} -DSTACK_SIZE_DEFAULT=256

${DPU_BINARY}: ${DPU_SOURCES} ${DPU_HEADERS} ${COMMONS_HEADERS}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} ${DPU_SOURCES} -o $@

###
### EXECUTION & TEST
###
run: all
	./${HOST_BINARY} -p ./datasets/integration/ > ${OUTPUT_FILE}
	cat ${OUTPUT_FILE}

check:
	cat ${OUTPUT_FILE} | grep "Match found" | diff datasets/integration/output.txt -

plotdata:
	echo "Mcc" > ${PLOTDATA_FILE}
	cat ${OUTPUT_FILE} | grep "average execution time" | sed 's/\[DPU\]  average execution time.*= .* ms (\(.*\) Mcc)/\1/' >> ${PLOTDATA_FILE}

%-check-format: %
	clang-format $< | diff -y --suppress-common-lines $< -

check-format: ${CHECK_FORMAT_DEPENDENCIES}

# better have two independent modules...
# pydoop_pipes and pydoop_hdfs

machine=`uname -m`
ifeq ($(machine),x86_64)
	OS_ARCH=amd64
else
	OS_ARCH=i386
endif
SHLIB_VERSION=1


PIPES_MODULE_NAME=pydoop_pipes
HDFS_MODULE_NAME=pydoop_hdfs


PYTHON_INC=/usr/include/python2.5
HADOOP_INC=/opt/hadoop/c++/Linux-amd64-64/include
HADOOP_LIB_DIR=/opt/hadoop/c++/Linux-amd64-64/lib

# this is, of course, gentoo specific.
# check that the jvm selected is the right one using java-config 
JAVA_HOME=/etc/java-config-2/current-system-vm
PLATFORM=linux

#---------------------------------------
PIPES_WRAP_FILES=pipes pipes_context pipes_test_support 
PIPES_AUX_FILES=HadoopPipes SerialUtils StringUtils hacked_wrapper
PIPES_CPP_AUX_FILES=$(addsuffix .cpp, ${PIPES_AUX_FILES})
PIPES_OBJ_AUX_FILES=$(addsuffix .o, ${PIPES_AUX_FILES})
PIPES_CPP_WRAP_FILES=$(addsuffix .cpp, ${PIPES_WRAP_FILES})
PIPES_HPP_WRAP_FILES=$(addsuffix .hpp, ${PIPES_WRAP_FILES})
PIPES_OBJ_WRAP_FILES=$(addsuffix .o, ${PIPES_WRAP_FILES})
PIPES_CPP_MAIN_FILE=${PIPES_MODULE_NAME}_main.cpp
PIPES_OBJ_MAIN_FILE=${PIPES_MODULE_NAME}_main.o

PIPES_ALL_INCS += -I${HADOOP_INC}
#ALL_LIBS += -L${HADOOP_LIB_DIR} -lhadooppipes -lhadooputils

PIPES_ALL_INCS += -I${PYTHON_INC}
PIPES_ALL_LIBS += -lpthread -lboost_python 

# 
#--

#-----------------------------------------
HDFS_WRAP_FILES=hdfs_fs hdfs_file
HDFS_AUX_FILES=
HDFS_CPP_AUX_FILES=$(addsuffix .cpp, ${HDFS_AUX_FILES})
HDFS_OBJ_AUX_FILES=$(addsuffix .o, ${HDFS_AUX_FILES})
HDFS_CPP_WRAP_FILES=$(addsuffix .cpp, ${HDFS_WRAP_FILES})
HDFS_HPP_WRAP_FILES=$(addsuffix .hpp, ${HDFS_WRAP_FILES})
HDFS_OBJ_WRAP_FILES=$(addsuffix .o, ${HDFS_WRAP_FILES})
HDFS_CPP_MAIN_FILE=${HDFS_MODULE_NAME}_main.cpp
HDFS_OBJ_MAIN_FILE=${HDFS_MODULE_NAME}_main.o

HDFS_ALL_INCS += -I./libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)
HDFS_ALL_LIBS +=-L./libhdfs -lhdfs

HDFS_ALL_INCS += -I${PYTHON_INC}
HDFS_ALL_LIBS += -lpthread -lboost_python 

LIBHDFS = ./libhdfs/libhdfs.so

CXXFLAGS= -g -fPIC ${PIPES_ALL_INCS} ${HDFS_ALL_INCS}


all: ${PIPES_MODULE_NAME}.so ${HDFS_MODULE_NAME}.so

%.o : %.cpp %.hpp
	g++ -c -o $@ ${CXXFLAGS} $<



./libhdfs/libhdfs.so:
	make OS_ARCH=$(OS_ARCH) SHLIB_VERSION=$(SHLIB_VERSION) -C ./libhdfs


${PIPES_MODULE_NAME}.so:  ${PIPES_OBJ_MAIN_FILE} $(PIPES_OBJ_WRAP_FILES)  $(PIPES_OBJ_AUX_FILES)
	g++ -shared  $^ ${PIPES_ALL_LIBS}  -o $@


${HDFS_MODULE_NAME}.so:  $(LIBHDFS) ${HDFS_OBJ_MAIN_FILE} $(HDFS_OBJ_WRAP_FILES)  $(HDFS_OBJ_AUX_FILES)
	g++ -shared  $^ ${HDFS_ALL_LIBS}  -o $@


${PIPES_CPP_MAIN_FILE} : ${PIPES_CPP_WRAP_FILES} ${PIPES_HPP_WRAP_FILES} 
	echo "#include <boost/python.hpp>" > $@
	for i in ${PIPES_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "void export_$${i}();" >> $@; fi; done
	echo "BOOST_PYTHON_MODULE(${PIPES_MODULE_NAME}){" >> $@
	for i in ${PIPES_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "export_$${i}();" >> $@; fi; done
	echo "}" >> $@


${HDFS_CPP_MAIN_FILE} : ${HDFS_CPP_WRAP_FILES} ${HDFS_HPP_WRAP_FILES} 
	echo "#include <boost/python.hpp>" > $@
	for i in ${HDFS_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "void export_$${i}();" >> $@; fi; done
	echo "BOOST_PYTHON_MODULE(${HDFS_MODULE_NAME}){" >> $@
	for i in ${HDFS_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "export_$${i}();" >> $@; fi; done
	echo "}" >> $@


clean:
	rm -rf *.o *~ ${PIPES_CPP_MAIN_FILE} ${HDFS_CPP_MAIN_FILE}

real-clean: clean
	rm -rf ${PIPES_CPP_MAIN_FILE} ${HDFS_CPP_MAIN_FILE} *.so
	make -C ./libhdfs real-clean

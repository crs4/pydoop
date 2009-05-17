MODULE_NAME=pydoop_core


PYTHON_INC=/usr/include/python2.5
HADOOP_INC=/opt/hadoop/c++/Linux-amd64-64/include
HADOOP_LIB_DIR=/opt/hadoop/c++/Linux-amd64-64/lib

# this is, of course, gentoo specific.
# check that the jvm selected is the right one using java-config 
JAVA_HOME=/etc/java-config-2/current-system-vm
PLATFORM=linux


# module low level components
PIPES_FILES=pipes pipes_context pipes_test_support 
PIPES_AUX_FILES=HadoopPipes SerialUtils StringUtils hacked_wrapper
#--
ALL_INCS += -I${HADOOP_INC}
#ALL_LIBS += -L${HADOOP_LIB_DIR} -lhadooppipes -lhadooputils

#--
HDFS_FILES= hdfs_fs hdfs_file
HDFS_AUX_FILES=
#--
ALL_INCS += -I./libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(PLATFORM)
ALL_LIBS +=-L./libhdfs -lhdfs

#
WRAP_FILES=${PIPES_FILES} ${HDFS_FILES}

# 
AUX_FILES=${PIPES_AUX_FILES} ${HDFS_AUX_FILES}

CPP_AUX_FILES=$(addsuffix .cpp, ${AUX_FILES})
OBJ_AUX_FILES=$(addsuffix .o, ${AUX_FILES})


CPP_WRAP_FILES=$(addsuffix .cpp, ${WRAP_FILES})
HPP_WRAP_FILES=$(addsuffix .hpp, ${WRAP_FILES})
OBJ_WRAP_FILES=$(addsuffix .o, ${WRAP_FILES})
CPP_MAIN_FILE=${MODULE_NAME}_main.cpp
OBJ_MAIN_FILE=${MODULE_NAME}_main.o

ALL_INCS += -I${PYTHON_INC}
ALL_LIBS += -lpthread -lboost_python 

CXXFLAGS= -g -fPIC ${ALL_INCS}


all: ${MODULE_NAME}.so

%.o : %.cpp %.hpp
	g++ -c -o $@ ${CXXFLAGS} $<



${MODULE_NAME}.so:  ${OBJ_MAIN_FILE} $(OBJ_WRAP_FILES)  $(OBJ_AUX_FILES)
	g++ -shared  $^ ${ALL_LIBS}  -o $@


${CPP_MAIN_FILE} : ${CPP_WRAP_FILES} ${HPP_WRAP_FILES} 
	echo "#include <boost/python.hpp>" > $@
	for i in ${WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "void export_$${i}();" >> $@; fi; done
	echo "BOOST_PYTHON_MODULE(${MODULE_NAME}){" >> $@
	for i in ${WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "export_$${i}();" >> $@; fi; done
	echo "}" >> $@


clean:
	rm -rf *.o *~ ${CPP_MAIN_FILE}

real-clean: clean
	rm -rf ${CPP_MAIN_FILE} *.so

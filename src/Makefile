# better have two independent modules...
# pydoop_core and pydoop_hdfs
CORE_MODULE_NAME=pydoop_core
HDFS_MODULE_NAME=pydoop_hdfs


PYTHON_INC=/usr/include/python2.5
HADOOP_INC=/opt/hadoop/c++/Linux-amd64-64/include
HADOOP_LIB_DIR=/opt/hadoop/c++/Linux-amd64-64/lib

# this is, of course, gentoo specific.
# check that the jvm selected is the right one using java-config 
JAVA_HOME=/etc/java-config-2/current-system-vm
PLATFORM=linux

#---------------------------------------
CORE_WRAP_FILES=pipes pipes_context pipes_test_support 
CORE_AUX_FILES=HadoopPipes SerialUtils StringUtils hacked_wrapper
CORE_CPP_AUX_FILES=$(addsuffix .cpp, ${CORE_AUX_FILES})
CORE_OBJ_AUX_FILES=$(addsuffix .o, ${CORE_AUX_FILES})
CORE_CPP_WRAP_FILES=$(addsuffix .cpp, ${CORE_WRAP_FILES})
CORE_HPP_WRAP_FILES=$(addsuffix .hpp, ${CORE_WRAP_FILES})
CORE_OBJ_WRAP_FILES=$(addsuffix .o, ${CORE_WRAP_FILES})
CORE_CPP_MAIN_FILE=${CORE_MODULE_NAME}_main.cpp
CORE_OBJ_MAIN_FILE=${CORE_MODULE_NAME}_main.o

CORE_ALL_INCS += -I${HADOOP_INC}
#ALL_LIBS += -L${HADOOP_LIB_DIR} -lhadooppipes -lhadooputils

CORE_ALL_INCS += -I${PYTHON_INC}
CORE_ALL_LIBS += -lpthread -lboost_python 

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



CXXFLAGS= -g -fPIC ${CORE_ALL_INCS} ${HDFS_ALL_INCS}


all: ${CORE_MODULE_NAME}.so ${HDFS_MODULE_NAME}.so

%.o : %.cpp %.hpp
	g++ -c -o $@ ${CXXFLAGS} $<


${CORE_MODULE_NAME}.so:  ${CORE_OBJ_MAIN_FILE} $(CORE_OBJ_WRAP_FILES)  $(CORE_OBJ_AUX_FILES)
	g++ -shared  $^ ${CORE_ALL_LIBS}  -o $@


${HDFS_MODULE_NAME}.so:  ${HDFS_OBJ_MAIN_FILE} $(HDFS_OBJ_WRAP_FILES)  $(HDFS_OBJ_AUX_FILES)
	g++ -shared  $^ ${HDFS_ALL_LIBS}  -o $@


${CORE_CPP_MAIN_FILE} : ${CORE_CPP_WRAP_FILES} ${CORE_HPP_WRAP_FILES} 
	echo "#include <boost/python.hpp>" > $@
	for i in ${CORE_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "void export_$${i}();" >> $@; fi; done
	echo "BOOST_PYTHON_MODULE(${CORE_MODULE_NAME}){" >> $@
	for i in ${CORE_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "export_$${i}();" >> $@; fi; done
	echo "}" >> $@


${HDFS_CPP_MAIN_FILE} : ${HDFS_CPP_WRAP_FILES} ${HDFS_HPP_WRAP_FILES} 
	echo "#include <boost/python.hpp>" > $@
	for i in ${HDFS_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "void export_$${i}();" >> $@; fi; done
	echo "BOOST_PYTHON_MODULE(${HDFS_MODULE_NAME}){" >> $@
	for i in ${HDFS_WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "export_$${i}();" >> $@; fi; done
	echo "}" >> $@


clean:
	rm -rf *.o *~ ${CORE_CPP_MAIN_FILE} ${HDFS_CPP_MAIN_FILE}

real-clean: clean
	rm -rf ${CORE_CPP_MAIN_FILE} ${HDFS_CPP_MAIN_FILE} *.so

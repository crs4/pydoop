MODULE_NAME=hadoop_pipes

# module low level components
WRAP_FILES=hadoop_pipes hadoop_pipes_context hadoop_pipes_test_support hacked_wrapper

# 
AUX_FILES=HadoopPipes SerialUtils StringUtils

CPP_AUX_FILES=$(addsuffix .cpp, ${AUX_FILES})
OBJ_AUX_FILES=$(addsuffix .o, ${AUX_FILES})


CPP_WRAP_FILES=$(addsuffix .cpp, ${WRAP_FILES})
HPP_WRAP_FILES=$(addsuffix .hpp, ${WRAP_FILES})
OBJ_WRAP_FILES=$(addsuffix .o, ${WRAP_FILES})
CPP_MAIN_FILE=${MODULE_NAME}_main.cpp
OBJ_MAIN_FILE=${MODULE_NAME}_main.o

PYTHON_INC=/usr/include/python2.5
HADOOP_INC=/opt/hadoop-0.19.1/c++/Linux-amd64-64/include
HADOOP_LIB_DIR=/opt/hadoop-0.19.1/c++/Linux-amd64-64/lib

CXXFLAGS= -g -fPIC -I${HADOOP_INC} -I${PYTHON_INC} 

HADOOP_LIBS=-lhadooppipes -lhadooputils
OTHER_LIBS=-lpthread


all: ${MODULE_NAME}.so

%.o : %.cpp %.hpp
	g++ -c -o $@ ${CXXFLAGS} $<



${MODULE_NAME}.so:  $(OBJ_WRAP_FILES) ${OBJ_MAIN_FILE}  $(OBJ_AUX_FILES)
	g++ -shared  $^ -L${HADOOP_LIB_DIR} ${HADOOP_LIBS} \
	${OTHER_LIBS} -lboost_python  -o $@

for i in hadoop_pipes hadoop_pipes_context hadoop_pipes_test_support hacked_wrapper ; do  if [ `grep -c export ${i}.cpp` != "0" ] ;  then echo $i; fi done

${CPP_MAIN_FILE} : ${CPP_WRAP_FILES} ${HPP_WRAP_FILES} 
	echo "#include <boost/python.hpp>" > $@
	for i in ${WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "void export_$${i}();" >> $@; fi; done
	echo "BOOST_PYTHON_MODULE(${MODULE_NAME}){" >> $@
	for i in ${WRAP_FILES} ; do if [ `grep -c export $${i}.cpp` != "0" ]; then echo "export_$${i}();" >> $@; fi; done
	echo "}" >> $@


clean:
	rm -rf *.o *~

real-clean: clean
	rm -rf ${CPP_MAIN_FILE} *.so

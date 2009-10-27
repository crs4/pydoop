WHAT="WordCount.cc"
HADOOP_HOME="/opt/hadoop"
#ARCH="amd64-64"
ARCH="i386-32"

g++ -o ${WHAT/.cc/} -I${HADOOP_HOME}/c++/Linux-${ARCH}/include ${WHAT} \
    -L${HADOOP_HOME}/c++/Linux-${ARCH}/lib -L/usr/lib \
    -lhadooppipes -lhadooputils -lpthread

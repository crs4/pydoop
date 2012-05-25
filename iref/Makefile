
#g++ -fPIC -shared -I/usr/include/python2.6  iref.cpp  -lboost_python -o iref.so

PY_V := $(shell python -c 'import sys; print "%s.%s" % sys.version_info[:2]')
CXXFLAGS := -fPIC -shared -I/usr/include/python$(PY_V)
LDLIBS := -lboost_python


build: iref.so

iref.so: iref.cpp
	$(CXX) $(CXXFLAGS) $< $(LDLIBS) -o $@

clean:
	rm -f iref.so

ACDC_SVN_BASE = ${HOME}/svn/ac-dc
COPYRIGHTER = $(ACDC_SVN_BASE)/tools/copyrighter/copyrighter.py
AUTHOR = "Simone Leo, Gianluigi Zanetti"
EXPORT_DIR = svn_export
GENERATED_SRC_FILES = src/_pipes_main.cpp src/_hdfs_main.cpp \
	src/SerialUtils.cc src/HadoopPipes.cc src/StringUtils.cc
BUILD_DIR := $(realpath .)/build
BUILD_LIB_DIR := $(BUILD_DIR)/lib
PYDOOP_DIR := $(BUILD_LIB_DIR)/pydoop

.PHONY: all build build_py install docs docs_py dist clean distclean

all: build
build: $(BUILD_DIR)
build_py: $(PYDOOP_DIR)


$(BUILD_DIR): setup.py pydoop src
	python $< build --build-base $(BUILD_DIR) --build-lib $(BUILD_LIB_DIR)

$(PYDOOP_DIR): setup.py pydoop
	python $< build_py --build-lib $(BUILD_LIB_DIR)

# 'setup.py install' does not accept --build-dir
install: build
	sudo python setup.py install_lib --skip-build --build-dir $(BUILD_LIB_DIR)
	sudo python setup.py install_egg_info

docs: build
	make -C docs html

docs_py: build_py
	make -C docs html

dist: docs
	svn export . $(EXPORT_DIR)
	python $(COPYRIGHTER) -a $(AUTHOR) -r $(EXPORT_DIR)
	python $(COPYRIGHTER) -a $(AUTHOR) -b '// BEGIN_COPYRIGHT' -e '// END_COPYRIGHT' -c "//" $(EXPORT_DIR)/src
	rm -rf $(EXPORT_DIR)/docs/*
	mv docs/_build/html $(EXPORT_DIR)/docs/
	cd $(EXPORT_DIR) && python setup.py sdist
	mv $(EXPORT_DIR)/dist .

clean:
	rm -rf build
	rm -fv $(GENERATED_SRC_FILES)
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;
	make -C docs clean
	make -C examples/self_contained clean

distclean: clean
	rm -rf dist $(EXPORT_DIR) docs/_build/*
	make -C examples/self_contained distclean

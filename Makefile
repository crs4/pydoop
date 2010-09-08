ACDC_SVN_BASE = ${HOME}/svn/ac-dc
COPYRIGHT_OWNER = CRS4
NOTICE_TEMPLATE = $(realpath .)/notice_template.txt
COPYRIGHTER = copyrighter -n $(NOTICE_TEMPLATE) $(COPYRIGHT_OWNER)
# install copyrighter >=0.2.0 from ac-dc/tools/copyrighter

EXPORT_DIR = svn_export
GENERATED_SRC_FILES = src/_pipes_main.cpp src/_hdfs_main.cpp \
	src/SerialUtils.cc src/HadoopPipes.cc src/StringUtils.cc
BUILD_DIR := $(realpath .)/build
BUILD_LIB_DIR := $(BUILD_DIR)/lib
PYDOOP_DIR := $(BUILD_LIB_DIR)/pydoop
DIST_DIR := $(realpath .)/$(EXPORT_DIR)/dist
DOCS_DIR := $(realpath .)/docs
DOCS_BUILD_DIR := $(DOCS_DIR)/_build

.PHONY: all build build_py install docs docs_py docs_put dist clean distclean

all: build
build: $(BUILD_DIR)
build_py: $(PYDOOP_DIR)
dist: $(DIST_DIR)
docs: $(DOCS_BUILD_DIR)

$(BUILD_DIR): setup.py pydoop src
	python $< build --build-base $(BUILD_DIR) --build-lib $(BUILD_LIB_DIR)

$(PYDOOP_DIR): setup.py pydoop
	python $< build_py --build-lib $(BUILD_LIB_DIR)

# 'setup.py install' does not accept --build-dir
install: build
	sudo python setup.py install_lib --skip-build --build-dir $(BUILD_LIB_DIR)
	sudo python setup.py install_egg_info

$(DOCS_BUILD_DIR): $(DOCS_DIR) build
	make -C $< html

docs_py: build_py
	make -C docs html

docs_put: docs
	rsync -avz --delete -e ssh $(EXPORT_DIR)/docs/html/ ${USER},pydoop@web.sourceforge.net:/home/groups/p/py/pydoop/htdocs/docs/

$(DIST_DIR): docs
	rm -rf $(EXPORT_DIR) && svn export . $(EXPORT_DIR)
	$(COPYRIGHTER) -r $(EXPORT_DIR)/pydoop $(EXPORT_DIR)/test $(EXPORT_DIR)/examples
	$(COPYRIGHTER) -r -c "//" $(EXPORT_DIR)/src
	rm -rf $(EXPORT_DIR)/docs/*
	mv docs/_build/html $(EXPORT_DIR)/docs/
	cd $(EXPORT_DIR) && python setup.py sdist

clean:
	rm -rf $(BUILD_DIR)
	rm -fv $(GENERATED_SRC_FILES)
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;
	make -C docs clean
	make -C examples/self_contained clean

distclean: clean
	rm -rf $(EXPORT_DIR) docs/_build/*
	make -C examples/self_contained distclean

ACDC_SVN_BASE = ${HOME}/svn/ac-dc
COPYRIGHTER = $(ACDC_SVN_BASE)/tools/copyrighter/copyrighter.py
AUTHOR = "Simone Leo, Gianluigi Zanetti"
EXPORT_DIR = svn_export
GENERATED_SRC_FILES = src/pydoop_pipes_main.cpp src/pydoop_hdfs_main.cpp \
	src/SerialUtils.cpp src/HadoopPipes.cpp src/StringUtils.cpp


.PHONY: all build install docs dist clean distclean

all: build

build:
	python setup.py build

install: build
	sudo python setup.py install --skip-build

docs: build
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

distclean: clean
	rm -rf dist $(EXPORT_DIR) docs/_build/*

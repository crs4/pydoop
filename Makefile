ACDC_SVN_BASE = ${HOME}/svn/ac-dc
COPYRIGHTER = $(ACDC_SVN_BASE)/tools/copyrighter/copyrighter.py
AUTHOR = "Simone Leo, Gianluigi Zanetti"
EXPORT_DIR = svn_export

# epydoc parameters
MODULES = pydoop/__init__.py \
	pydoop/pipes.py \
	pydoop/input_split.py \
	pydoop/factory.py \
	pydoop/utils.py \
	pydoop/hdfs.py

NAME = pydoop
DOC_DIR = doc/html
URL = http://pydoop.sourceforge.net

.PHONY: all clean dist distclean

all: dist

dist:
	svn export . $(EXPORT_DIR)
	python $(COPYRIGHTER) -a $(AUTHOR) -r $(EXPORT_DIR)
	python $(COPYRIGHTER) -a $(AUTHOR) -b '// BEGIN_COPYRIGHT' -e '// END_COPYRIGHT' -c "//" $(EXPORT_DIR)/src
	mkdir -p $(EXPORT_DIR)/doc/html
	cd $(EXPORT_DIR) && epydoc --parse-only -v -n $(NAME) -o $(DOC_DIR) -u $(URL) $(MODULES)
	cd $(EXPORT_DIR) && python setup.py sdist

clean:
	cd pygsa && rm -f *.pyc */*.pyc

distclean:
	rm -rf $(EXPORT_DIR) build
	rm -fv MANIFEST src/SerialUtils.cpp src/HadoopPipes.cpp src/pydoop_pipes_main.cpp src/pydoop_hdfs_main.cpp src/StringUtils.cpp
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

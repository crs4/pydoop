EXPORT_DIR = /tmp/pydoop_export
COPYRIGHT_OWNER = CRS4
NOTICE_TEMPLATE = notice_template.txt
COPYRIGHTER = copyrighter -n $(NOTICE_TEMPLATE) $(COPYRIGHT_OWNER)
# install copyrighter >=0.4.0 from ac-dc/tools/copyrighter

GENERATED_SRC_FILES = $(wildcard src/*_main.cpp) $(wildcard src/*.cc)

.PHONY: all build build_py install install_py install_user install_user_py docs docs_py docs_put docs_view dist clean distclean

all: build

build:
	python setup.py build

build_py:
	python setup.py build_py

install: build
	python setup.py install --skip-build

install_py: build_py
	python setup.py install --skip-build

install_user: build
	python setup.py install --skip-build --user

install_user_py: build_py
	python setup.py install --skip-build --user

docs:
	make -C docs html

docs_py:
	make -C docs html

docs_put: docs
	rsync -avz --delete -e ssh docs/_build/html/ ${USER},pydoop@web.sourceforge.net:/home/project-web/pydoop/htdocs/docs/

docs_view: docs
	yelp docs/_build/html/index.html &

dist-svn: docs
	rm -rf $(EXPORT_DIR) && svn export . $(EXPORT_DIR)
	$(COPYRIGHTER) -r $(EXPORT_DIR)
	rm -rf $(EXPORT_DIR)/docs/*
	mv docs/_build/html $(EXPORT_DIR)/docs/
	cd $(EXPORT_DIR) && python setup.py sdist

dist: docs
	rm -rf $(EXPORT_DIR) && mkdir $(EXPORT_DIR) && cp -a * $(EXPORT_DIR)
	$(COPYRIGHTER) -r $(EXPORT_DIR)
	rm -rf $(EXPORT_DIR)/docs/*
	mv docs/_build/html $(EXPORT_DIR)/docs/
	(cd $(EXPORT_DIR) && python setup.py sdist) && mv -i $(EXPORT_DIR)/dist/pydoop-*.tar.gz .

clean:
	rm -rf build
	rm -f $(GENERATED_SRC_FILES)
	make -C docs clean
	make -C examples/self_contained clean
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\)' -exec rm -fv {} \;

distclean: clean
	rm -rf $(EXPORT_DIR)
	make -C examples/self_contained distclean

PYTHON := python
TEMPDIR := $(shell mktemp -u)
GIT_REV_FN = GIT_REV
WHEEL_DIR=./dist
PY_V := $(shell ${PYTHON} -c 'import sys; print("%d.%d" % sys.version_info[:2])')

TARGETS=all build wheel install install_user install_wheel install_wheel_user\
        docs docs_py docs_view \
        clean distclean uninstall_user logo favicon
.PHONY: $(TARGETS)

all:
	@echo "Try one of: ${TARGETS}"

install_user: build
	${PYTHON} setup.py install --user

install: build
	${PYTHON} setup.py install

build:
	${PYTHON} setup.py build

wheel:
	${PYTHON} setup.py bdist_wheel --dist-dir=${WHEEL_DIR}

install_wheel: wheel
	pip install --use-wheel --no-index --pre --find-links=${WHEEL_DIR} pydoop

install_wheel_user: wheel
	pip install --use-wheel --no-index --pre --find-links=${WHEEL_DIR} pydoop --user

logo: docs/_static/logo.png

favicon: docs/_static/favicon.ico

docs/_static/logo.png: logo/logo.svg
#	direct conversion to final size with inkscape does not look good
	inkscape -z -D -f $< -e logo/logo.png -w 800 # -b '#ffffff'
	convert -resize 200x logo/logo.png $@
	rm -f logo/logo.png

docs/_static/favicon.ico: logo/favicon.svg
	inkscape -z -D -f $< -e favicon-256.png -w 256 -h 256
	for i in 16 32 64 128; do \
	  convert favicon-256.png -resize $${i}x$${i} favicon-$${i}.png; \
	done
	convert favicon-16.png favicon-32.png favicon-64.png favicon-128.png $@
	rm -f favicon-*.png

# pydoop must be installed for sphinx autodoc to work
docs: install_user logo favicon
	make -C docs html

docs_py: install_user_py logo favicon
	make -C docs html

docs_view: docs
	yelp docs/_build/html/index.html &

dist: docs
	./dev_tools/git_export -o $(TEMPDIR)
	git rev-parse HEAD >$(TEMPDIR)/$(GIT_REV_FN)
	rm -rf $(TEMPDIR)/docs/*
	mv docs/_build/html $(TEMPDIR)/docs/
	cd $(TEMPDIR) && ${PYTHON} setup.py sdist
	mv -i $(TEMPDIR)/dist/pydoop-*.tar.gz .
	rm -rf $(TEMPDIR)

clean:
	${PYTHON} setup.py clean --all
	rm -rf pydoop.egg-
	rm -f docs/_static/logo.png docs/_static/favicon.ico
	make -C docs clean
	make -C examples clean
	find . -regex '.*\(\.pyc\|\.pyo\|~\|\.so\|\.jar\|\.class\)' -exec rm -fv {} \;

uninstall_user:
	rm -rf ~/.local/lib/python$(PY_V)/site-packages/pydoop*
	rm -f ~/.local/bin/pydoop

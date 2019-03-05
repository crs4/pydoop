FROM crs4/pydoop-docs-base

COPY . /build/pydoop
WORKDIR /build/pydoop

RUN ${PYTHON} -m pip install --no-cache-dir --upgrade -r requirements.txt \
    && ${PYTHON} setup.py build \
    && ${PYTHON} setup.py install --skip-build \
    && ${PYTHON} setup.py clean \
    && inkscape -z -D -f logo/logo.svg -e logo.png -w 800 2>/dev/null \
    && convert -resize 200x logo.png docs/_static/logo.png \
    && inkscape -z -D -f logo/favicon.svg -e 256.png -w 256 -h 256 2>/dev/null \
    && for i in 16 32 64 128; do \
        convert 256.png -resize ${i}x${i} ${i}.png; \
    done \
    && convert 16.png 32.png 64.png 128.png docs/_static/favicon.ico \
    && for a in script submit; do \
        ${PYTHON} dev_tools/dump_app_params --app ${a} -o docs/pydoop_${a}_options.rst; \
    done \
    && make SPHINXOPTS="-W" -C docs html

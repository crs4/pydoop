# Build example -- This will be fixed in a future release

export HADOOP_HOME=/ELS/els5/acdc/opt/hadoop
PREFIX=/ELS/els5/acdc/opt

python setup.py build_clib
python setup.py build_ext -L${PREFIX}/lib -I${PREFIX}/include -R${PREFIX}/lib
python setup.py build_py
python setup.py install --skip-build

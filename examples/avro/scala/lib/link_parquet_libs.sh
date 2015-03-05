#!/bin/bash

# 
parquet_src_root=../../../../..

for module in incubator-parquet-{format,mr}
do
    find ${parquet_src_root}/${module} -name '*.jar' -exec ln -s {} \;
done

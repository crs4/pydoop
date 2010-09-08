#!/bin/bash

find . -regex '.*\(\.class\|~\)' -exec rm -fv {} \;
rm -fv *.jar

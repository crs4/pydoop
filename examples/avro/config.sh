[ -n "${PYDOOP_AVRO_EXAMPLES:-}" ] && return || readonly PYDOOP_AVRO_EXAMPLES=1

TARGET="target"
export CLASS_DIR="${TARGET}/classes"
export CP_PATH="${TARGET}/cp.txt"
export JAR_PATH="${TARGET}/pydoop-avro-examples.jar"

gen_classpath() {
    [ -f "${CP_PATH}" ] && return 0
    mkdir -p "${TARGET}"
    mvn dependency:resolve
    mvn dependency:build-classpath -D mdep.outputFile="${CP_PATH}"
    echo -n ':'$(readlink -e ../../lib)/'*' >> "${CP_PATH}"
}

export -f gen_classpath

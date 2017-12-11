"""\
Perform full substitution on the Pydoop script template and check
it with flake8.

Any options (i.e., arguments starting with at least a dash) are passed
through to flake8.
"""

import sys
import os
import tempfile

from flake8.main.cli import main as flake8_main


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(THIS_DIR, os.pardir, "pydoop", "app"))
from script_template import DRIVER_TEMPLATE


def main(argv):
    code = DRIVER_TEMPLATE.substitute(
        module="module",
        map_fn="map_fn",
        reduce_fn="reduce_fn",
        combine_fn="combine_fn",
        combiner_wp="None",
    )
    fd = None
    try:
        fd, fn = tempfile.mkstemp(suffix=".py", text=True)
        os.write(fd, code.encode("utf-8"))
    finally:
        if fd is not None:
            os.close(fd)
    flake8_argv = [fn] + [_ for _ in argv if _.startswith("-")]
    try:
        flake8_main(flake8_argv)
    finally:
        os.remove(fn)


if __name__ == "__main__":
    argv = sys.argv[1:]
    if set(argv).intersection(["-h", "--help"]):
        print(__doc__)
    else:
        main(argv)

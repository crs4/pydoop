# BEGIN_COPYRIGHT
# END_COPYRIGHT

from distutils.core import setup
import cv


setup(
    name="cv",
    version=cv.__version__,
    description="MR app for counting occurrence of each vowel in input text",
    author=cv.__author__,
    author_email=cv.__author_email__,
    url=cv.__url__,
    packages=["cv", "cv.lib", "cv.mr"]
    )

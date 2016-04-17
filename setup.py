from setuptools import setup

setup(
  name = 'redis_scripts',
  py_modules = ['redis_scripts'],
  version = '0.0.1',
  description = 'Higher-level redis Lua scripting',
  author = 'Leif K-Brooks',
  author_email = 'eurleif@gmail.com',
  url = 'https://github.com/leifkb/redis_scripts',
  keywords = ['redis', 'lua'],
  classifiers = [
    "Development Status :: 3 - Alpha",
    "Topic :: Database",
    "License :: OSI Approved :: MIT License"
  ],
)

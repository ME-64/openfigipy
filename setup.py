from setuptools import setup
import pathlib

# python3 setup.py sdist bdist_wheel
# twine upload dist/*

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()


setup(
  name = 'openfigipy',
  packages = ['openfigipy'],
  version = '0.0.2',
  license='MIT',
  description = 'A python wrapper around the Open FIGI API that leverages pandas DataFrames',
  long_description = README,
  long_description_content_type = 'text/markdown',
  author = 'ME-64',
  author_email = 'milo_elliott@icloud.com',
  url = 'https://github.com/ME-64/openfigipy',
  keywords = ['API wrapper', 'Financial Reference Data', 'Open FIGI', 'openfigi', 'bloomberg', 'figi'],
  include_package_data = True,
  install_requires=['pandas', 'ratelimit', 'cachetools'],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.8',
  ],
)

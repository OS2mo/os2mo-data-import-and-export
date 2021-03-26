from setuptools import setup

setup(
    name='metacli',
    version='0.1',
    py_modules=['metacli'],
    install_requires=['Click'],
    entry_points='''
        [console_scripts]
        metacli=metacli:cli
    ''',
)
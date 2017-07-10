import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.txt')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.txt')) as f:
    CHANGES = f.read()

requires = [
    'pyramid',
    'pyramid_tm',
    'pyramid_chameleon',
    'pyramid_debugtoolbar',
    'waitress',
    'deform',
    'pyramid_deform',
]

tests_require = [
    'WebTest >= 1.3.1',  # py3 compat
    'pytest',
    'pytest-cov',
]

corpus_require = [
    'bs4',
    'pyldavis',
    'click',
    'wordcloud',
    'rq',
]


setup(
    name='eea.corpus',
    version='0.1',
    description='EEA Corpus Server',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        'Programming Language :: Python',
        'Framework :: Pyramid',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Application',
    ],
    author='',
    author_email='',
    url='',
    keywords='web pyramid pylons',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    extras_require={
        'testing': tests_require,
    },
    install_requires=requires+corpus_require,
    entry_points={
        'paste.app_factory': [
            'main=eea.corpus:main',
        ],
        'console_scripts': [
            'worker=eea.corpus.async:worker',
        ]
    },
)

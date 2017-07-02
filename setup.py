from setuptools import setup

setup(**{
    'name': 'tornado-unity',
    'version': '0.1.beta0',
    'author': 'Alexey Poryadin',
    'author_email': 'alexey.poryadin@gmail.com',
    'description': 'This module help to build uniting multiprocessing tornado apps.',
    'packages': ['unity'],
    'install_requires': [
        'tornado>=4.3',
    ],
    'license': 'MIT'
})

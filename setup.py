from setuptools import setup

setup(
    name='estimize',
    version='1.0',
    description='Estimize research module.',
    entry_points={
        'console_scripts': [
            'estimize = estimize.__main__:main',
        ],
    },
    url='http://estimize.com',
    author='Estimize',
    author_email='sales@estimize.com',
    license='MIT',
    packages=['estimize'],
    zip_safe=False
)

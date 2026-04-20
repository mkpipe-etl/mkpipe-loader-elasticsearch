from setuptools import setup, find_packages

setup(
    name='mkpipe-loader-elasticsearch',
    version='0.3.0',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe', 'elasticsearch>=8.0'],
    include_package_data=True,
    entry_points={
        'mkpipe.loaders': [
            'elasticsearch = mkpipe_loader_elasticsearch:ElasticsearchLoader',
        ],
    },
    description='Elasticsearch loader for mkpipe.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)

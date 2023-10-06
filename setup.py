from setuptools import find_packages, setup

setup(
    name="molly",
    version="0.1",
    author="Flavia Ouyang",
    author_email="hello@flaviaouyang.com",
    description="Data quality monitoring library",
    url="",
    python_requires=">=3.7, <4",
    packages=["molly"],
    install_requires=[
        "sqlalchemy>=2.0.0",
        "psycopg2-binary>=2.9.9",
        "pandas>=2.1.1",
    ],
)

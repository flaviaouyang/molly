from setuptools import setup

setup(
    name="molly",
    version="0.1",
    author="Flavia Ouyang",
    author_email="flavia.ouyang@mail.mcgill.ca",
    description="Data quality monitoring library",
    url="",
    python_requires=">=3.7, <4",
    packages=["molly"],
    include_package_data=True,
    install_requires=[
        "sqlalchemy>=2.0.0",
        "pandas>=2.1.1",
        "tabulate>=0.9.0",
        "multimethod>=1.11",
        "psycopg2>=2.9",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.2",
            "black>=23.9.1",
            "isort>=5.13",
        ]
    },
)

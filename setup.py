"""Setup trapi-throttle package."""
from setuptools import setup

setup(
    name="trapi-throttle",
    version="1.0.0",
    author="Patrick Wang",
    author_email="patrick@covar.com",
    url="https://github.com/ranking-agent/trapi-throttle",
    description="TRAPI throttle",
    packages=["trapi_throttle"],
    include_package_data=True,
    install_requires=[
        "httpx>=0.18.0",
        "reasoner-pydantic>=1.1.2.1,<1.1.3",
    ],
    zip_safe=False,
    license="MIT",
    python_requires=">=3.9",
)

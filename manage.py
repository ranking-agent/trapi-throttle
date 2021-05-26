#!/usr/bin/env python3
import sys
import os


def print_green(s):
    GREEN = '\033[92m'
    ENDC = '\033[0m'
    print(f"{GREEN}{s}{ENDC}")


def run_command(cmd):
    print_green(cmd)
    os.system(cmd)


def dev(extra_args):
    """
    This command starts up a development server through Docker
    """
    command = f"""\
    docker build -t trapi-throttle -f Dockerfile .
    docker run -p 7830:7830 -it trapi-throttle {extra_args}\
    """

    run_command(command)


def test(extra_args):
    """
    This command runs the tests within docker-compose
    and then exits.
    """
    command = f"""\
    docker-compose -f docker-compose.test.yml up\
    --build --abort-on-container-exit {extra_args}
    """
    run_command(command)


def coverage(extra_args):
    """
    Run tests in docker, copy out a coverage report,
    and display in browser
    """
    command = f"""\
    docker rm trapi-throttle-testing || true
    docker build -t trapi-throttle-testing \
                 -f Dockerfile.test .
    docker run --name trapi-throttle-testing trapi-throttle-testing \
            pytest --cov trapi-throttle/ --cov-report html {extra_args}
    docker cp trapi-throttle-testing:/app/htmlcov /tmp/trapi-throttle-cov/
    open /tmp/trapi-throttle-cov/index.html
    """
    run_command(command)


def profile(extra_args):
    """
    Profile a test in docker, copy out a report,
    and display using the snakeviz utility
    """
    command = f"""\
    docker rm trapi-throttle-profile || true
    docker build -t trapi-throttle-profile \
                 -f Dockerfile.test .
    docker run --name trapi-throttle-profile trapi-throttle-profile \
            python -m cProfile -o trapi-throttle.prof -m pytest {extra_args}
    docker cp trapi-throttle-profile:/app/trapi-throttle.prof /tmp/
    snakeviz /tmp/trapi-throttle.prof
    """
    run_command(command)


def lock(extra_args):
    """
    Write requirements-lock.txt and requirements-test-lock.txt
    """
    requirements_files = {
        "requirements.txt": "requirements-lock.txt",
        "requirements-test.txt": "requirements-test-lock.txt",
    }

    for src, locked in requirements_files.items():
        command = f"""\
        docker run -v $(pwd):/app python:3.9 \
            /bin/bash -c "pip install -qqq -r /app/{src} && pip freeze" > {locked}
        """
        run_command(command)


def main():
    command = sys.argv[1]
    command_func = globals()[command]
    extra_args = " " + " ".join(sys.argv[2:])
    command_func(extra_args)


if __name__ == '__main__':
    main()

#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = totes_awesome
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}:${WD}/src
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source ./venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)
	$(call execute_in_env, $(PIP) install -r ./requirements.txt -t dependencies/python)

################################################################################################################
# Set Up

## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install flake8
flake8:
	$(call execute_in_env, $(PIP) install flake8)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)
	$(call execute_in_env, $(PIP) install pytest-cov)

## Set up dev requirements (bandit, black)
dev-setup: bandit flake8 coverage

# Build / Run

## Run bandit
run-bandit:
	$(call execute_in_env, bandit -r src/)

## Run flake8
run-flake8:
	$(call execute_in_env, flake8  ./src/*.py ./test/*.py)

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest test/ -v)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src test/)

## Run all checks
run-checks: run-bandit run-flake8 unit-test check-coverage

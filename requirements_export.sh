#!/bin/bash

poetry export -f requirements.txt --output requirements.txt --without-hashes --with dev

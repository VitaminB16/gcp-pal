#!/bin/bash

# Requires pytest-xdist (pip install pytest-xdist)
python -m pytest tests -n 12 --dist worksteal
python clean_up.py

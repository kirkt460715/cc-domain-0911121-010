#!/bin/sh
set -eu
python -m compileall -q chainledger demo_support
python -m unittest discover -s tests -v
python -m chainledger demo
python demo_support/check.py
python -I demo_support/mfg_drill.py

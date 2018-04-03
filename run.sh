#!/bin/bash

SRC_DIR="./src"
ENTRY_POINT=$SRC_DIR/edgar_sessionizer/main.py

PYTHONPATH=$PYTHONPATH:$SRC_DIR python $ENTRY_POINT
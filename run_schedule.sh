#!/bin/bash
nohup uv run main.py schedule "$1" > schedule.log 2>&1 &

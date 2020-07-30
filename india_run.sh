#!/bin/bash 

cd india
python india_agent_csv_creator.py
cd ..
cd python
python dgen_model.py
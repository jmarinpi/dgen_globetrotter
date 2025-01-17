=================
Generating Agents
=================

India Example
-------------
The India and Colombian versions of dGen Globetrotter involve creating agents as flat-files (`.csv`) that are then processed by a modified version of the dGen codebase. 

Generating agents is an integral part of the modeling process and involves using known data and our assumptions to statistically sample representative agents that can be simulated.

For dGen India, `india/india_agent_csv_creator.py` controls this process.

An example for generating agents for India is as follows:
- adjust any agent configuration settings in `india/agent_config.py` such as electricity cost increases.
- rerun `python india_agent_csv_creator.py` to create new agents. 
- each function in `india_agent_csv_creator.py` controls the output of a `.csv` file with a single scope. For instance, `rate_escalations()` creates the `india_base/rate_escalations.csv` file that is read in by dGen.
- if new data is available for sampling, `.csv` files can be edited directly (if this is easier), or the underlying functions in `india_agent_csv_creator.py` can be modified. 
- the new files are created within `india/india_base/` and this directory is also copied into `input_scenarios/india_base/`
- after the model has been run, net load analysis can be conducted in `india/notebooks/india_plots.ipynb`.

If new historic adoption data is available, a seperate notebook `india_bass_estimation.ipynb` creates the `pv_bass.csv` file, after being manually tuned for parameter bounds. 

Agent sampling for India involves constructing normal distributions of observed feature attributes, and sampling from these for each agent. The number of agents sampled is the same per state, and defined by `agent_config.py:AGENTS_PER_GEOGRAPHY`.



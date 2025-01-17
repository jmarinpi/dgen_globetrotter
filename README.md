
dGen - International (Globetrotter)
=========
Master repository for international serverless versions of dGen. Ideally, this should act as a development branch for all international versions, with agnositic implementation and localization present in input sheets, and config flags if necessary. For U.S. applications of dGen, please see the current [Open Sourced version of dGen](https://github.com/NREL/dgen).

To Setup the Model (Mexican Example)
====================================

	1. Copy reference_data/example_data/mex_high_costs.xlsm to input_scenarios and rename to a unique scenario name.

	2. Copy reference_data/example_data/mex_high_costs input csv folder to input_scenarios as well, then rename folder to the a unique name.

	3. Open input_scenarios/mex_high_costs.xlsm in Microsoft Excel and manually customize settings. Settings in this folder apply to all control regions and states equally. Make sure to update the Scenario Folder Name on the main tab to the name of the input csv folder. Note that you can use the same input csv folder across multiple input spreadsheets.

	4. Check each file in the copied input csv folder, adjusting control region and state level parameters where necessary to model a particular scenario. Note there is addtional data in the reference_data folder to use is determining settings.

	5. From the diffusion/python folder in a command line window, run:
	 	$ python dgen_model.py 
	
		Note you may need to run pip install -r requirements.txt first to ensure all required Python packages are installed. 

	6. As the model runs the name of the output folder (based on the current time) will be printed on the screen. Find these results in the runs folder. 

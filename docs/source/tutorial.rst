========
Tutorial
========

High-level utilization of dGen can be done in four steps:

1) Setting parameters in an :file:`.xlsm` (Microsoft Excel) file.
-----------------------------------------------------------------

A file with default values is provided by dGen, located at :file:`reference_data/example_data/mex_high_costs.xlsm`. Opening this file in Excel reveals several sheets (tabs). The ‘Main’ tab is the most direct way to change high-level parameters, Including the Analysis End Year, the Sectors (markets) to run analysis for, Load Growth projections, Rate Structure and Escalation, Market Adoption Curves, Carbon Pricing Schemes, and Net Metering Schemes. Advanced users running custom scenarios can also point dGen towards a custom Pre-Generated Agents file in the **Main** tab. 

Other tabs include Market Projections, which contains a table of macro-economic projections including an Annual Inflation value, and yearly parameters for Carbon Price, (if a price is identified in ‘Main’), and Sectoral Rate Escalations (based on a 2015 base year). 

The **Financing** Tab includes financing assumptions for specific projects. These include Loan Tenor (length), Loan Rates, Down Payments, Discount Rates, and Tax Rates. An additional table in the ‘Financing’ tab includes Sectoral Depreciation schedules. 

The **Storage** tab contains battery storage benchmark prices on a per kWh and per kW installed basis for each sector. 

.. note::

    dGen’s Storage Module (:func:`python.storage_functions`) is currently disabled as it was not included in the scope of Globetrotter implementation.

The **Solar** tab contains PV system benchmark prices on a a per kW installed basis for each sector. Fixed and Variable Annual Operations and Maintenance (O&M), PV Degradation rates can also be set. Power Density assumes that technological advances will lead to increased cell efficiencies, this doesn’t directly affect system costs, but does allow agents with smaller roofs to adopt a higher capacity if economic.

Once modified with the user’s desired parameters, this file (or an alternative) should be copied into the :file:`input_data/` folder. When initializing dGen, each scenario file within :file:`input_data/` will be run, one after another. 

2) Placing or editing :file:`.csv` data files
---------------------------------------------

Along with the `.xlsm` file in :file:`input_data/`, dGen requires a folder containing a number of :file:`.csv` files with specific data. Like :file:`mex_high_costs.xlsm`, a default set of files is available in :file:`reference_data/example_data/mex_high_costs`. More details on these files and their contents are available in the `Scenario Data` section of this documentation. The :file:`mex_high_costs/` folder, or an alternative, must be copied into :file:`input_data/` alongside :file:`mex_high_costs.xlsm`.

3) Running the model through a command line interface (CLI) or interactive development environment (e.g. a Jupyter Notebook)
----------------------------------------------------------------------------------------------------------------------------

Once the correct environment is installed and the correct scenario data has been placed within :file:`input_data/`, the model is ready to be initialized. 

Running the model on the **command line** involves moving to the :file:`python/` directory within the dGen package. From inside the :file:`python/` folder, activate the virtualenv set-up during installation. Finally, run the model as a python directive.::

	#enter python directory
	$ cd <your path here>/dgen_globetrotter/python

	#activate the installed environment
	$ source env/Scripts/activate  #or conda activate env
	
	#run the model
	(env)$ python dgen_model.py #run the model

As the model run is progressing, the status will be logged to the screen. This information allows the user to see which Scenario and Year are currently being run.

Some users might find it helpful to run dGen within an interactive development environment, such as a Jupyter Notebook. If you do not have Jupyter installed, follow the `Installing Jupiter using Anaconda and conda <https://jupyter.readthedocs.io/en/latest/install.html#id3>`_ instructions.

.. note::
   You will likely not see anything printed to the screen while running dGen from within an IDE.

4) Interpreting Results
-----------------------

After running the model, a :file:`runs/` folder has been created within dGen, a results folder with a timestamp for the date and time that the model was initialized contains a folder for each scenario that was run. Opening the scenario folder contains the following files:

- **dpv_MW_by_ba_and_year.csv** contains the installed MW capacity of distributed photovoltaics by year for each control area.
- **dpv_cf_by_ba.csv** contains an hourly profile (8760) for the last model year of the distributed photovoltaic capacity factor.
- **agent_outputs.csv** contains the output agent dataframe, which allows users to view agnate-by-agent outcomes for dpv adoption. Multiple years of model runs have been appended on as additional rows.
- **agent_df_base.pkl** is a python pickled file containing the input agent dataframe.
- **Copies of the input files** including an :file:`.xlsm` and an :file:`input_data/` folder containing :file:`.csv` files, described in :ref:`Input Data`
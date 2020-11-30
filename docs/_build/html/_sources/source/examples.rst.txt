=================
Example Use Cases
=================

Basic Use Case
--------------
Basic usage of the dGen model can take place with minimum programming skills. Most input data can be modified within :file:`.csv` files or :file:`.xlsm` files through a program like Microsoft Excel. 

For some applications of dGen with a limited number of agents, like dGen Mexico, it can be possible to run the model on a laptop or desktop computer. However running the model on a High Performance Computing system will likely decrease the model run time for all applications and in the case of model runs with a large number of agents, it can be necessary to avoid memory issues.

Let’s demonstrate usage of dGen by testing a simple hypothesis: If retail electricity rate escalations are high, we will see more commercial solar capacity than if rate escalations are low. In this hypothesis, we are testing if self-generation will be more economic for customers than paying high retail rates, especially when programs like net metering are available to compensate excess generation sold back to the utility. 

To test this hypothesis, we’ll conduct two model runs:

#. *Our test case:* with high rate escalation and net metering based on the avoided cost of generation. 

#. *Our control case:* with low rate escalation and net metering based on the avoided cost of generation. 

To set up a model run, we need to place a scenario :file:`.xlsm` file and a folder of :file:`.csv` files containing input data in the :file:`input_scenarios/` folder. An example of this is within the :file:`reference_data/example_data/` folder, called :file:`mex_high_costs.xlsm`, and :file:`mex_high_costs/`. Copy these folders to :file:`input_scenarios/` and rename them :file:`high_rate_escalation.xlsm` and :file:`example_input_csvs/`.

Next, the most impactful parameters should be changed within the :file:`high_rate_escalation.xlsm` file. Open the file with Microsoft Excel, and select the `Main` tab.

Change the following options:

* Rename the :py:const:`Scenario Name` to be ‘high_rate_escalation’.

* Rename the :py:const:`Scenario Folder` to be the sibling file of :file:`.csv` files, in our case it should be ‘example_input_csvs’.

* :py:const:`Analysis End Year` should be ‘2026’ (it can be up to 2050, but we’ll reduce it to decrease model run time for this example).

* :py:const:`Markets` should be ‘Only Commercial’. 

* `Load Growth Scenario` should be ‘Planning’.

* The :py:const:`Res`, :py:const:`Com`, and :py:const:`Ind Rate Structure` fields should all stay as ‘Flat (Annual Average)’.

* :py:const:`Com Rate Escalation` should be ‘High’, as that is the scenario we are testing, but :py:const:`Res` and :py:const:`Ind` should stay as ‘Planning’. 

* Different data sources are available for :py:const:`Max Market Curve`, we’ll use ‘Navigant’ for :py:const:`Com`. 

* A Carbon Price is not part of our scenario, so we’ll set :py:const:`Carbon Price` to ‘No Carbon Price’. 

* We’re testing a :py:const:`Net Metering Scenario` based on the ‘Avoided Cost’.

.. note::
   The ‘Tariff Rate’ option for :py:const:`Net Metering Scenario` is not currently working. 

Once these parameters are input, save the file and exit Excel. 

We now need to copy the :file:`high_rate_escalation.xlsm` file and paste a second copy into :file:`input_scenarios/`. Let’s rename this copy to be :file:`low_rate_escalation.xlsm`. We don’t need to recopy the :file:`example_input_csvs/` folder. 

Open :file:`low_rate_escalation.xlsm` with Microsoft Excel and select the `Main` tab:

* Change :py:const:`Scenario Name` to be ‘low_rate_escalation’

* Change :py:const:`Res Rate Escalation` to be ‘Low’. 

Save file and close it. 

Now we’re ready to run the model. This involves opening your computer’s command line interface (CLI). On Macs this should be an application called Terminal, on Windows this should be called Command Prompt. 

Running the model on the **command line** involves moving to the :file:`python/` directory within the dGen package. From inside the :file:`python/` folder, activate the virtualenv set-up during installation. If an environment hasn’t been created yet, follow the :ref:`Installation` instructions. Finally, run the model as a python directive.::

	#enter python directory of dgen_globetrotter
	$ cd <your path here>/dgen_globetrotter/python

	#activate the installed environment
	$ source env/Scripts/activate  #or conda activate env
	
	#run the model
	(env)$ python dgen_model.py #run the model

Model results are saved to the :file:`runs/` folder, inside a folder with a time-stamped name, will be a log and code profile, and a folder for each scenario. There should have a folder called :file:`high_rate_escalation` and a file called :file:`low_rate_escalation`. 

* Inside each of these folders will be a pickled Pandas DataFrame containing complete model information for each agent. 

* Additionally, :file:`agent_outputs.csv` contains the agent_df for each year joined on to each other length wise. 

* :file:`dpv_cf_by_ba.csv` contains an 8760 of solar capacity factors for each control region in a representative year of the model. 

* :file:`dpv_MW_by_ba_and_year.csv` contains the cumulative installed solar capacity for each control region in each model year.

* Finally, copies of the original :file:`input_data/` folder, and excel scenario file are included for reference. 

.. note::
   Reading pickled DataFrames is easy with::

      pandas.read_pickle(<path>)

Now that our model has run, let’s open :file:`runs/<timestamp>/low_rate_escalation/dpv_MW_by_ba_and_year.csv`

.. csv-table:: Basic Use Case: Low Rate Escalation
   :file: doc_data/dpv_MW_basic_low.csv
   :stub-columns: 1
   :header-rows: 1

For the sake of this example, an additional ’Total’ column and row were added so we can see that **7750.8 MW** of commercial solar PV was installed during the ‘low_rate_escalation’ run. Let’s now compare that with :file:`runs/<timestamp>/high_rate_escalation/dpv_MW_by_ba_and_year.csv`.

.. csv-table:: Basic Use Case: High Rate Escalation
   :file: doc_data/dpv_MW_basic_high.csv
   :stub-columns: 1
   :header-rows: 1
 
In the high_rate_escalation scenario **8092.9 MW** of commercial solar PV were adopted, an increase of **342.1 MW**!


Intermediate Use Case
---------------------
Keeping the :file:`low_rate_escalation.xlsm` and :file:`high_rate_escalation.xlsm` files, let’s change some more advanced parameters. We’ll keep the low rate escalation scenario as our control, but we’ll see what happens to installed commercial solar PV capacity when we increase the Investment Tax Credit (ITC), which is a subsidy that is paid on the installed cost of the system.


Apart from the *Main* tab where we modified scenario settings previously, tabs exist for *Market Projections*, *Financing*, *Storage (Note: Storage is not functional in dGen Mexico)*, and *Solar* within the :file:`high_rate_escalaiton.xlsm` file. To change the ITC, let’s switch to the *Financing* tab. Under the *Commercial* section of the *Financing* table, we can examine the ‘Solar ITC %’ column to see that the default Commercial ITC is 30% in 2014, and begins phasing down in 2020 until it reaches 10% in 2024. Let’s test a scenario where new legislation was passed to expand the ITC to 35% between 2020 and 2026. To do this, change the respective values to ‘0.35’. Let’s save this as a new file, :file:`high_rate_escalation_ITC.xlsm`. We’ll keep all our changes in the *Main* tab, and leave :file:`/low_rate_escalation` file untouched so it can act as our control.

We run the model the same way as in our previous example, although you might want to move the `low_rate_escalation.xlsm` and :file:`high_rate_escalation.xlsm` files out of the :file:`input_scenarios/` folder, so that they are excluded from this run. Once the model run is complete, we can open :file:`runs/<timestamp>/hih_rate_escalation_ITC/dpv_MW_by_ba_and_year.csv` to see the new installed solar PV commercial capacity. 

.. csv-table:: Intermediate Use Case: High Rate Escalation with 35% ITC in 2020
   :file: doc_data/dpv_MW_intermediate_ITC.csv
   :stub-columns: 1
   :header-rows: 1

Wow! We see **9296.7 MW** of adoption when the ITC is expanded, clearly that is an impactful policy. Try playing around with changing other values in the tabs in :file:`high_rate_escalation_ITC.xlsm`.

Advanced Use Case
-----------------
At a more advanced level, individual :file:`.csv` files within the :file:`input_scearios/example_input_csvs/` folder can be changed. :ref:`Input Data` covers what each of these file is. Let’s try changing some of these values now, starting with :file:`example_input_csvs/pv_bass.csv`. 

At the core of the dGen model is a bass diffusion model. The bass diffusion curve is an economic principle that identifies an S-curve of adoption for new technologies. Even if it is incredibly economic for a commercial entity to adopt solar, market research indicates that many will still not adopt because of unawareness, fear of change, or other barriers to entry. The S-curve of the Bass Diffusion model anticipates that a small number of initial adopters will inspire a larger number of followers. This is true for many things within our economy. For instance, if a new independent-film comes out that is very good, a small number of initial moviegoers might see it because they are un-afraid to try new things, or some members might stumble into the movie on a whim but end up enjoying the film. As these initial viewers talk to their friends and family and tell them about the great film they recently saw, others will be inspired to go see the movie. This trend has been observed with technology adoption too, including the adoption rate for the automobile, smartphones, and now roof-top solar PV. 

The curve is defined by two parameters, *p* and *q*.

* *p* is the coefficient of innovation, it identifies the proportion of the population that will spontaneously install solar PV in each time increment. 

* *q* is the coefficient of imitation, it identifies the proportion of the population that will adopt based on current levels of adoption.

Mathematically, the model is expressed as:

.. math::
   \frac{f(t)}{1 - F(t)} = p + qF(t)


Put simply:[#f1]_ 

.. math:: 
   \scriptsize
   \text{customers who will purchase at time }t = (p * \text{ remaining potential}) + (q * \text{ number of adopters} * \text{remaining potential})

.. image:: doc_data/example_bass.png
   :scale: 25 %
   :align: center

Applying this to the dGen model, if we open :file:`input_scenarios/example_input_csvs/pv_bass.csv` we’ll find a file containing *p* and *q* parameters for every state, control region, and sector combination. These have been prepopulated based on our careful analysis of measured historic data. For the sake of this example, let’s input some crude values in order to test a scenario where more commercial agents will spontaneously adopt solar, perhaps this could be the result of a massive advertisement campaign. To do this, we want to raise the *p* values for the commercial sector. Let’s change all of these values to ‘0.0075’ which is around 3.5 times higher than the current average p value.

After saving the file and running the model again (just for the ‘high_rate_escalation_ITC’ scenario), we get the following results for installed commercial solar PV capacity:

.. csv-table:: Advanced Use Case: High Rate Escalation with 35% ITC in 2020 and p values of ‘0.0075’
   :file: doc_data/dpv_MW_advanced_ITC_p.csv
   :stub-columns: 1
   :header-rows: 1

Increasing the *p* value has lead to **15,008.9 MW** of commercial solar capacity. Obviously the *p* parameters is an extremely significant input. This example use case is purposefully extreme: it is testing a scenario with constant high retail rate escalations, a major legislative push for a 35% ITC, and a significantly higher willingness of commercial customers to adopt solar. While more realistic scenarios would not involve changing these values this drastically, hopefully this example demonstrated the types of levers that are available within the dGen model and how to access them.

Try playing with other values both in the excel file, and folder of :file:`.csv` files to test other scenarios. If you have any questions about using the dGen model, please contact Ben.Sigrin@NREL.gov.   

.. rubric:: Footnotes
.. [#f1] Bar Ilan University. “Forecasting the Sales of New Products and the Bass Model.” https://faculty.biu.ac.il/~fruchtg/829/lec/6.pdf
 
diffusion
=========

Repository for the Distributed Wind Diffusion Model. Includes related SQL, Python, R, and Documentation.


## How to:
All instructions below should be completed from the command line (cmd.exe or Cygwin in Windows, Terminal in OSX).

Set Up Your Python Environment
----------------------------------
These instructions should only completed when the you receive a "VersionError" warning from the model.

- Make sure the environment doesn't exist
```shell
conda remove -n dgen_pyenv --all
```
- Move into the python folder
```shell
cd ./python
```
- Create the new environment
```shell
conda env create -f environment.yml
```

Running the Model in Interactive Mode from Spyder
-------------------------------------------------
- Activate the virtual environment
```shell
source activate dgen_pyenv
```
- Launch Spyder
```shell
spyder
```
- When finished, close Spyder
- Deactivate the virtual environment
```shell
source deactivate
```

Running the Model on the Linux Server
-------------------------------------
- ssh to the compute node
```shell
ssh cn05.bigde.nrel.gov
# enter network password when prompted
```
- Start a screen session
```shell
screen
# optionally, use screen -S myname to name the session
```
- Activate the virtual environment
```
source activate dgen_pyenv
```
- Run the model
```
python dgen_model.py
```
- When finished, deactivate virtual environment
```
source deactivate
```
- Close the screen session
exit

Updating Packages within the Python Virtual Environment
-------------------------------------------------------
- Activate virtual environment
```shell
source activate dgen_pyenv
```
- Run conda install or update for your package that you want to update
- Export the updated virtual environment to file
```shell
conda env export > diffusion/python/environment.yml
```
- Push changes to diffusion/python/environment.yml to github
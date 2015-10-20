diffusion
=========

Repository for the Distributed Wind Diffusion Model. Includes related SQL, Python, R, and Documentation.


## How to:
----------
All instructions below should be completed from the command line (cmd.exe or Cygwin in Windows, Terminal in OSX).

Set Up Your Python Environment
----------------------------------
These instructions should only completed when the you receive a "VersionError" warning from the model.

1. Make sure the environment doesn't exist
```shell
conda remove -n dgen_pyenv --all
```
2. Move into the python folder
```shell
cd ./python
```
3. Create the new environment
```shell
conda env create -f environment.yml
```

Running the Model in Interactive Mode from Spyder
-------------------------------------------------
1. Activate the virtual environment
```shell
source activate dgen_pyenv
```
2. Launch Spyder
```shell
spyder
```
3. When finished, close Spyder
4. Deactivate the virtual environment
```shell
source deactivate
```

Running the Model on the Linux Server
-------------------------------------
1. ssh to the compute node
```shell
ssh cn05.bigde.nrel.gov
# enter network password when prompted
```
2. Start a screen session
```shell
screen
# optionally, use screen -S myname to name the session
```
3. Activate the virtual environment
```
source activate dgen_pyenv
```
4. Run the model
```
python dgen_model.py
```
5. When finished, deactivate virtual environment
```
source deactivate
```
6. Close the screen session
exit

Updating Packages within the Python Virtual Environment
-------------------------------------------------------
1. Activate virtual environment
```shell
source activate dgen_pyenv
```
2. Run conda install or update for your package that you want to update
3. Export the updated virtual environment to file
```shell
conda env export > diffusion/python/environment.yml
```
4. Push changes to diffusion/python/environment.yml to github
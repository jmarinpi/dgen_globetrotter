diffusion
=========

Repository for the Distributed Wind Diffusion Model. Includes related SQL, Python, R, and Documentation.

# Python Environment Setup
- Note: All instructions should be complete from the command line (cmd.exe or Cygwin in Windows, Terminal in OSX)

# Set Up a Conda Virtual Environment
- Note: This is only necessary when the you receive a "VersionError" warning from the model 
- make sure the environment doesn't exist
```
conda remove -n dgen_pyenv --all
```
- move into the python folder
```
cd ./python
```
- create the new environment
```
conda env create -f environment.yml
```

# Running the Model in Interactive Mode from Spyder
- activate the virtual environment
```
source activate dgen_pyenv
```
- launch Spyder
```
spyder
```
- When finished, close Spyder
- deactivate virtual environment
```
source deactivate
```

# Running the Model on the Linux Server
- ssh to the compute node
```
ssh cn05.bigde.nrel.gov
# enter network password when prompted
```

- start a screen session
```
screen
# optionally, use screen -S myname to name the session
```

- activate the virtual environment
```
source activate dgen_pyenv
```

- run the model
```
python dgen_model.py
```

- when finished, deactivate virtual environment
```
source deactivate
```

- close screen session
exit


# Updating packages within the virtual environment
- activate virtual environment
```
source activate dgen_pyenv
```

- run conda install or update for your package that you want to update

- export the updated virtual environment to file
```
conda env export > diffusion/python/environment.yml
```

- push changes to diffusion/python/environment.yml to github
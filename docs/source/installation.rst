============
Installation
============

dGen requires a Python 2.7 installation. Some computers (Macs and Linux) might already have this installed.

To ensure that the correct version of python is installed before proceeding run the following through a command line interface: ::

	$ python --version
	Python 2.7.5

If python is not installed, or an incorrect version is installed, it is recommended to install python using `Anaconda <https://docs.anaconda.com/anaconda/install/>`_.

Downloading dGen
----------------

At this point, dGen is not open-sourced. If you have access to the dGen Mexico model, you should already have a GitHub account and should have access to the `GitHub Repo <https://github.com/NREL/dgen_mexico>`_.

To download dGen, open your command line interface and run the following command.::
	
	# Replace <USERNAME> with your GitHub username
	$ git clone https://<USERNAME>@github.com/NREL/dgen_mexico.git

You will be asked for your GitHub password unless you have an ssh-key set-up. After entering your password, the dGen model will be downloaded into a new :file:`dgen_mexico/` folder. 


Set-up a Virtual Environment
----------------------------

Once it is confirmed that Python 2.7 is installed and dGen is downloaded, specific package versions must be installed based on the :file:`python/requirements.txt`. The easiest way to ensure that the correct package versions are installed is by setting up a new environment. 

**Virtualenv**

Virtualenv is a package to create isolated Python environments. This is likely the easiest way to setup dGen. ::
	
	$ pip install virtualenv
	
	#create a new environment
	$ virtualenv env #a different name can be substituted for env
	
	#activate the environment
	$ source env/Scripts/activate

	#move to the ‘python’ folder within ‘dgen_mexico/‘
	$ cd python

	#load the required Python packages from a file
	(env)$ pip install -r requirements.txt

**Anaconda**

If you have familiarity using Anaconda, it is also possible to setup a new Python 2.7 environment through conda. ::

	#create a new environment
	$ conda create —name py2 python=2.7

	#activate the environment	
	$ condo activate py2

	#move to the ‘python’ folder within ‘dgen_mexico/‘
	$ cd python

	#load the required Python packages from a file
	(py2)$ conda install —file requirements.txt

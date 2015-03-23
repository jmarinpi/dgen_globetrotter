Prereqs:
- request access to Developer VPN:
	- I had success with this working through Scott Drennan
	- you need to be (1) added to the Dev VPN User Group (this requires Line Manager approval with a reason -- you can justify it by saying something like this:
		[Employee Name] writes software to perform analysis and is pulling in libraries form a variety of web sites using automated dependency acquisition (python, R, docker). Many times the NREL firewall blocks this, which I assume was the reason for the development of the developer VPN, and why I am requesting access for [Employee Name].
	- you then need (2) to have Dev VPN added to your Junos software on your laptop. You can do this yourself:
		Type: UAC or SSL-VPN
		Name: Developer VPN
		Server URL: https://devvpn.nrel.gov/developers
 
- Setting up Docker on MAC
(https://docs.docker.com/installation/mac/)

- install boot2docker on mac
	- download and install from https://github.com/boot2docker/boot2docker
- start boot2docker from terminal:
	- boot2docker init (only need to do this once)
	- boot2docker start
		- follow instructions to change EVIRONMENT vars, e.g.:
		    export DOCKER_HOST=tcp://192.168.59.103:2376
		    export DOCKER_CERT_PATH=/Users/mgleason/.boot2docker/certs/boot2docker-vm
		    export DOCKER_TLS_VERIFY=1
		    ** these vars will go away when you restart, so to set them permanently:
		    - cd to ~
		    - open .bash_profile in text editor
		    	- from command line: open -a "Sublime Text 2" .bash_profile 
		    - add the lines above (changing to match your computer ip and username, etc)
	- boot2docker shellinit
- next time you run it, you'll just need to do:
	- boot2docker restart
	- boot2docker shellinit




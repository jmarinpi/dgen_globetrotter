#Development best practices:
- changes made to the database need to be replicated on both gispgdb and dnpdb001
	- if you make changes to only one of the two databases, open a new issue with high priority, assigned to mgleason, indicating what you changed and on which database. provide a commit hash, if appropriate.
- if you open an issue, be sure to assign it each of the following:
	- a milestone
	- a priority level
	- an assignee
- if you hack a solution into the code (e.g., loading something from csv instead of the database, manually excluding something from a sql query ,etc.), open a new issue referencing the commit hash, assigned to mgleason

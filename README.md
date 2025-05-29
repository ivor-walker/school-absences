# School’s Out: Analysing Pupil Absence Data
## Abstract
Understanding drivers behind pupil attendance enables governments to reward the highest performing schools, support those that are struggling, and ultimately prioritise resources towards initiatives that affect the most change in pupil attendance. This project is a console and web-based application in Python and Apache Spark designed to analyse a large dataset of pupil absence in schools in England from 2006-2018. 

## Local usage
The web version is hosted at [ivorwalker.co.uk/school-absences](ivorwalker.co.uk/school-absences), but this analysis can also be run locally. The console application can be started by running "main-terminal.py", and the Flask server can be started by running a WGSI server (e.g. waitress) on "main_flask.py". 

A .env file needs to be created in the same directory as main_flask.py, see "sample_env.txt". Data needs to be provided to run locally at the directory specified in .env. The exact dataset I used is [Absence by geography, 2018-19](https://explore-education-statistics.service.gov.uk/find-statistics/pupil-absence-in-schools-in-england/2018-19/data-guidance), but any year of absence by geography data should work.

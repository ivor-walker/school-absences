# School’s Out: Analysing Pupil Absence Data
## Abstract
Understanding drivers behind pupil attendance enables governments to reward the highest performing schools, support those that are struggling, and ultimately prioritise resources towards initiatives that affect the most change in pupil attendance. This project is a console and web-based application in Python and Apache Spark designed to analyse a large dataset of pupil absence in schools in England from 2006-2018. 

## Local usage
The web version is hosted at ivorwalker.co.uk/schools-out, but this analysis can also be run locally. The console application can be started by running "main-terminal.py", and the Flask server can be started by running "main-flask.py". 

Data needs to be provided to run locally at the directory specified in [src/controller.py](https://github.com/ivor-walker/school-absences/blob/bb4995e4bf6f6272cff2ce73fcc13c36201054be/src/model/absences.py#L26), by default at /src/data/. The exact dataset I used is [Absence by geography, 2018-19](https://explore-education-statistics.service.gov.uk/find-statistics/pupil-absence-in-schools-in-england/2018-19/data-guidance), but any year of absence by geography data should work.

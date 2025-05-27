import sys, os;

# Find path of this school-absences.wsgi
project_path = os.path.dirname(os.path.abspath(__file__));

# Place project directory on PYTHONPATH and CD into it 
if project_path not in sys.path:
    sys.path.insert(0, project_path);
os.chdir(project_path);

# Set environmental variables
# Set app location
os.environ.setdefault("APPLICATION_ROOT", "/school-absences");

# Set data location
os.environ.setdefault("DATA_LOC", "/src/data/Absence_3term201819_nat_reg_la_sch.csv");

# Set debug mode
os.environ.setdefault("DEBUG", "False");

# Set Flask app name
os.environ.setdefault("FLASK_APP", "main_flask.py");

from main_flask import app as application;

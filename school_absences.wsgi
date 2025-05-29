# WSGI file for running the Flask application with Apache
# Load environment variables from .env file
from dotenv import load_dotenv;
load_dotenv();

import sys, os;

# CD into the src directory
# Find path of this school-absences.wsgi
project_path = os.path.dirname(os.path.abspath(__file__));

# Place project directory on PYTHONPATH and CD into it 
if project_path not in sys.path:
    sys.path.insert(0, project_path);
    os.chdir(project_path);

# Import Flask app
from src.views.flaskapp import app as application;

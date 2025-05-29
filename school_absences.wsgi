# WSGI file for running the Flask application with a WSGI server and Apache

# CD into the src directory
# Find path of this school-absences.wsgi
project_path = os.path.dirname(os.path.abspath(__file__));

# Place project directory on PYTHONPATH and CD into it 
if project_path not in sys.path:
    sys.path.insert(0, project_path);
    os.chdir(project_path);

# Import Flask app, wrapped in a WSGI application
from main_flask import application;

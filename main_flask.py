"""
Entrypoint for Flask server
Exposes an application callable for WSGI servers
For different WSGI servers:
waitress: waitress-serve --listen=0.0.0.0:5000 main_flask:application
"""
# Load environment variables from .env file
from dotenv import load_dotenv;
load_dotenv();

import os;

from werkzeug.middleware.dispatcher import DispatcherMiddleware;
from src.views.flaskapp import app as flask_app;


# Create an application that's importable by the WSGI server
application = DispatcherMiddleware(
    # No root app here
    {},
    {
        # Flask app
        os.environ["APPLICATION_ROOT"] : flask_app,
    }
);

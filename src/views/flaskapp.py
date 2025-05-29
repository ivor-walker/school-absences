import sys, os;

from flask import Flask, render_template;

from src.controller import Controller;

"""
Entrypoint for running as Flask server, handles initialisation and routing 
"""

# Initialise controller & flask app
view_type = "flask";
view_debug = os.environ.get("DEBUG_VIEW", "False") == "True";
controller = Controller(view_type, 
    csv_loc = os.environ["DATA_LOC"],
    view_debug = view_debug
);

app = Flask(__name__, 
    template_folder = "templates",
    static_folder = "static",
);

app.config["APPLICATION_ROOT"] = os.environ["APPLICATION_ROOT"];

app.secret_key = os.environ["FLASK_SECRET"];

controller.set_flask_app(app);

# Routing to all functions in controller
@app.route("/")
def menu():
    return controller.display_menu();

@app.route("/la_enrolment", methods = ["GET", "POST"])
def la_enrolment():
    return controller.get_enrolment_by_la_over_time();

@app.route("/auth_school_type", methods = ["GET", "POST"])
def auth_scool_type():
    return controller.get_auth_by_school_type();

@app.route("/auth_school_type_detailed", methods = ["GET", "POST"])
def auth_scool_type_detailed():
    return controller.get_auth_by_school_type_detailed();

@app.route("/unauth_la_region", methods = ["GET", "POST"])
def get_unauth_by_la_region():
    return controller.get_unauth_by_la_region();

@app.route("/la_year", methods = ["GET", "POST"])
def compare_la_in_year():
    return controller.compare_la_in_year();

@app.route("/region_attendance_time", methods = ["GET", "POST"])
def compare_region_attendance_over_time():
    return controller.compare_region_attendance_over_time();

@app.route("/eda_school_type_location_absences", methods = ["GET", "POST"])
def eda_school_type_location_absences():
    return controller.eda_school_type_location_absences();

@app.route("/model_school_type_location_absences", methods = ["GET", "POST"])
def model_school_type_location_absences():
    return controller.model_school_type_location_absences(display_results = True);

@app.route("/model_school_type_location_absences_detailed", methods = ["GET", "POST"])
def model_school_type_location_absences_detailed():
    return controller.model_school_type_location_absences(display_detailed_results = True);

# Run the Flask app
if __name__ == "__main__":
    # Get debug mode from environment variable
    debug_mode = os.environ.get("DEBUG", "False") == "True";

    app.run(
        debug = debug_mode,
    ),

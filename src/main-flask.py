from flask import Flask, render_template;

from controller import Controller;

"""
Entrypoint for running as Flask server, handles initialisation and routing 
"""
# Initialise controller & flask app
view_type = "flask";
controller = Controller(view_type);

app = Flask(__name__, template_folder = "views/templates");
app.secret_key = "...";
controller.set_flask_app(app);

# Routing to all functions in controller
@app.route("/")
def menu():
    return controller.display_menu();

@app.route("/la_enrolment", methods = ["GET"])
def la_enrolment():
    return controller.get_enrolment_by_la_over_time();

@app.route("/auth_school_type", methods = ["GET"])
def auth_scool_type():
    return controller.get_auth_by_school_type();

@app.route("/auth_school_type_detailed", methods = ["GET"])
def auth_scool_type_detailed():
    return controller.get_auth_by_school_type_detailed();

@app.route("/unauth_la_region", methods = ["GET"])
def get_unauth_by_la_region():
    return controller.get_unauth_by_la_region();

@app.route("/la_year", methods = ["GET"])
def compare_la_in_year():
    return controller.compare_la_in_year();

@app.route("/region_attendance_time", methods = ["GET"])
def compare_region_attendance_over_time():
    return controller.compare_region_attendance_over_time();

@app.route("/eda_school_type_location_absences", methods = ["GET"])
def eda_school_type_location_absences():
    return controller.eda_school_type_location_absences();

@app.route("/model_school_type_location_absences", methods = ["GET"])
def model_school_type_location_absences():
    return controller.model_school_type_location_absences(display_results = True);

@app.route("/model_school_type_location_absences_detailed", methods = ["GET"])
def model_school_type_location_absences_detailed():
    return controller.model_school_type_location_absences(display_detailed_results = True);

# Run the Flask app
if __name__ == "__main__":
    app.run(debug = True);

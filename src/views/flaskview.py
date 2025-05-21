"""
View using Flask
"""

from flask import Flask, render_template

class FlaskView:
    def __init__(self):
        # Define menu to routing map
        self.__routes = {
            "1": "/la_enrolment",
            "2": "/auth_school_type",
            "3": "/auth_school_type_detailed",
            "4": "/unauth_la_region",
            "5": "/la_year",
            "6": "/region_attendance_time",
            "7": "eda_school_type_location_absences",
            "8": "model_school_type_location_absences",
            "9": "model_school_type_location_absences_detailed",
        };
    
    """
    Set the Flask app instance, set by entrypoint
    """
    def set_app(self, app):
        self.__app = app;

    """
    Append a line to current view
    """
    def display_line(self, text):
        # TODO
        ();

    """
    Display menu template
    """
    def display_menu(self, menu):
        self.__app.render_template('menu.html', menu=menu, routes=self.__routes);

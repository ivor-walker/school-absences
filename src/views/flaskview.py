"""
View using Flask
"""

from flask import Flask, render_template, request;
from utils.earlyresponse import EarlyResponse;

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

        # Define server states
        self.__prompts = [];

    """
    Set the Flask app instance, set by entrypoint and passed to the view via controller
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
        return render_template('menu.html', menu=menu, routes=self.__routes);

    """
    Take in a user prompt and output a form, or process data
    """
    def prompt_user(self,
        prompts = None,
        types = None,
    ):
        datas = request.args;
        
        # Render additional form and return it to user       
        if not datas:
            html = render_template('form.html', prompts_types = list(zip(prompts, types)));
            raise EarlyResponse(html);
        
        # Else, extract data and return it to controller
        else:
            datas_keys_values = [(key, value) for key, value in datas.to_dict(flat = False).items() if key != 'submit']; 
            datas_values = [key_value[1] for key_value in datas_keys_values];

            return datas_values;

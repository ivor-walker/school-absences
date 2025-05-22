"""
View using Flask
"""

from flask import Flask, render_template, request;
from utils.earlyresponse import EarlyResponse;
from utils.typevalidation import convert_type;

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
            self.__last_prompts_types = list(zip(prompts, types));
            html = render_template(
                'form.html', 
                prompts_types = self.__last_prompts_types,
                len_responses = 0,
            );
            raise EarlyResponse(html);
        
        # Else, extract data and return it to controller
        else:
            return self.__extract_responses(datas, join_members = False, type_match = True);
    
    """
    Helper method to extract data from form response
    """
    def __extract_responses(self, datas, join_members = True, type_match = False, list_split_char = ","):
        datas_keys_values = [(key, value) for key, value in datas.to_dict(flat = False).items() if key != 'submit']; 

        # Need to convert from string to target type for controller
        if type_match:
            for i in range(len(datas_keys_values)):
                target_type = self.__last_prompts_types[i][1]; 
                target_value = datas_keys_values[i][1][0];

                datas_keys_values[i] = (datas_keys_values[i][0], convert_type(target_value, target_type));

        # Each response is a 1-element list, need to join them before sending to client

        datas_values = [key_value[1] for key_value in datas_keys_values];
        if join_members:
            datas_values = ["".join(value) for value in datas_values];

        return datas_values;
    
    # TODO get both views to use below method 
    

    """
    Display an error in most recent form
    """
    def display_error(self, error):
        responses = self.__extract_responses(request.args);

        return render_template(
            'form.html', 
            prompts_types = self.__last_prompts_types,
            responses = responses, 
            len_responses = len(responses),
            error = str(error)
        );

    """
    Append a line to current form
    """
    def display_line(self, text):
        responses = self.__extract_responses(request.args);
        return render_template(
            "form.html",
            prompts_types = self.__last_prompts_types,
            responses = responses,
            len_responses = len(responses),
            text = text
        );

    """
    Display a Spark dataframe fetched by controller
    """
    def display_frame(self, frame):
        responses = self.__extract_responses(request.args);
        
        # Convert to Pandas dataframe and display as HTML table
        html_table = frame.toPandas().to_html(
            classes = "data", 
            header = "true", 
            index = False
        );

        return render_template(
            "form.html",
            prompts_types = self.__last_prompts_types,
            responses = responses,
            len_responses = len(responses),
            html_table = html_table
        );

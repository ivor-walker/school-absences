"""
View using Flask
"""

from src.views.baseview import BaseView;

from flask import Flask, render_template, request;
from src.utils.earlyresponse import EarlyResponse;
from src.utils.typevalidation import convert_type;

import base64;
from io import BytesIO;

class FlaskView(BaseView):
    def __init__(self):
        # Define menu to routing map
        self.__routes = {
            "1": "/la_enrolment",
            "2": "/auth_school_type",
            "3": "/auth_school_type_detailed",
            "4": "/unauth_la_region",
            "5": "/la_year",
            "6": "/region_attendance_time",            
            "7": "/eda_school_type_location_absences",
            "8": "/model_school_type_location_absences",
            "9": "/model_school_type_location_absences_detailed",
        };

        # Define server states
        self.__app = None;
        self.__user_navigation_map = {};
        
    """
    Set the Flask app instance, set by entrypoint and passed to the view via controller
    """
    def set_app(self, app):
        self.__app = app;

        # Add base routes from app to routing menu
        self.__app_route = self.__app.config["APPLICATION_ROOT"];

        for route in self.__routes:
            self.__routes[route] = self.__app_route + self.__routes[route];

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
        # If user needs to input data, send user form 
        datas = request.form;
        if len(datas) == 0:
            prompts_types = list(zip(prompts, types));

            html = render_template(
                'form.html', 
                prompts_types = prompts_types,
            );
            raise EarlyResponse(html);
        
        # Else, extract data and return it to controller
        return self.__extract_responses(datas, join_members = False);

    """
    Helper method to extract data from form response
    """
    def __extract_responses(self, datas, join_members = True, list_split_char = ","):
        # Get web resposes as key-value tuples, excluding the submit button
        datas_keys_values = [(key, value) for key, value in datas.to_dict(flat = False).items() if key != 'submit']; 

        # Need to convert from string to target type for controller
        self.__convert_types(datas_keys_values, list_split_char = list_split_char); 

        # Each response is a 1-element list, need to join them before sending to client
        datas_values = [key_value[1] for key_value in datas_keys_values];
        if join_members:
            datas_values = ["".join(value) for value in datas_values];
        
        return datas_values;
        
    """
    Convert types of responses to match controller expectations
    """
    def __convert_types(self, datas_keys_values, list_split_char = ","):
        for i, key_value in enumerate(datas_keys_values):
            key, value = key_value;

            target_type = key.split("_")[0];
            target_value = value[0];

            datas_keys_values[i] = (key, convert_type(target_value, target_type));

        return datas_keys_values;


    """
    Display an error in most recent form
    """
    def display_error(self, error):
        return render_template(
            'form.html', 
            error = str(error)
        );

    """
    Append a line to current form
    """
    def display_line(self, text, no_form = False):
        return render_template(
            "form.html",
            text = text
        );

    """
    Display a Spark dataframe fetched by controller
    """
    def display_frame(self, frame, no_form = False):
        # Convert to Pandas dataframe and display as HTML table
        html_table = self.__frame_to_html(frame); 

        return render_template(
            "form.html",
            html_table = html_table
        );

    """
    Display multiple frames
    """
    def display_multiple_frames(self, frames, titles, no_form = False):
        html_tables = self.__frames_to_html(frames, titles); 

        return render_template(
            "form.html",
            html_table = html_tables
        );

    
    """
    Helper method to convert a Spark dataframe to HTML via Pandas
    """
    def __frame_to_html(self, frame, title = None):
        if title is None:
            title_template = ""; 
        else:
            title_template = f"<h3>{title}</h3>"

        return title_template + frame.toPandas().to_html(
            classes = "data", 
            header = "true", 
            index = False
        );

    """
    Helper method to convert multiple Spark dataframes to HTML via Pandas
    """
    def __frames_to_html(self, frames, titles):
        html_tables = [self.__frame_to_html(frame, title = title) for frame, title in zip(frames, titles)];
        html_tables = "".join(html_tables);

        return html_tables;

    
    """
    Render graphs to form
    """
    def display_figures(self,
        frames = None,
        figures = None,
        titles = None,
        no_form = False
    ):
        responses = self.__extract_responses(request.args); 

        # Convert to Pandas dataframe and display as HTML table
        html_tables = self.__frames_to_html(frames, titles);

        # Show each figure in a separate div
        figures_html = "";
        for fig, title in zip(figures, titles):
            # Define smaller figure size for HTML
            fig.set_figwidth(10);
            fig.set_figheight(7.5);
            
            # Create a buffer to save the figure
            buffer = BytesIO();
            fig.savefig(buffer, format='png');

            # Extract the image data from the buffer as base64
            data = base64.b64encode(buffer.getvalue()).decode('utf-8');

            # Provide base64 as image data to client
            figures_html += f"<h3>{title}</h3><img src='data:image/png;base64,{data}'/>";

        return render_template(
            "form.html",
            html_table = html_tables,
            figures = figures_html 
        );

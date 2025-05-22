from model.absences import Absences;

from views.terminalview import TerminalView;
from views.flaskview import FlaskView;

from utils.earlyresponse import EarlyResponse;
from functools import wraps;

import random;

"""
Class to handle the menu for the user to interact with the data
"""
class Controller:
    def __init__(self, view_type, view_debug = True):
        print("Loading view...");

        # Instantiate data and view
        if view_type == "terminal":
            self.__view = TerminalView();
        elif view_type == "flask":
            self.__view = FlaskView();
        
        print("View loaded, loading data and menu (this will take a while)...");
        if view_debug == False:
            self.__absences = Absences(); 
        
            # Set default user inputs
            self.__defaults = self.__absences.get_default_values();

        # Define menu
        self.__menu = {
            "1": "Task 1C: Get enrolment, by local authority, over time",
            "2": "Task 1D: Get authorised absences, by school type, in a given year",
            "3": "Task 1D Extension: Get exact reasons for absence, by school type, in a given year",
            "4": "Task 1E: Get unauthorised absences, by local authority or region, in a given year",
            "5": "Task 2A: Chart and get various performance statistics, by local authorities, in a given year",
            "6": "Task 2B: Chart and get overall absence rates, by region, over time",
            "7": "Task 3, Part 1: Chart and get overall absences rates, region and school type",
            "8": "Task 3, Part 2: Model absences, by school type and region, and display results",
            "9": "Task 3, Part 3: Model absences, by school type and region, and display detailed results",
        };

        # Add an option to exit the menu
        if view_type == "terminal":
            self.__menu["0"] = "Exit";

        print("Loading complete!");

    """
    Expose view's display menu method
    """
    def display_menu(self):
        return self.__view.display_menu(self.__menu);
    
    """
    Expose view's display error method
    """
    def display_error(self, error):
        return self.__view.display_error(error);

    """ 
    Wrap around controller methods, to catch an early response from the view, i.e.. an error or a template needs to be rendered
    """

    def catch_early_response(fn):
        # Define a wrapper to wrap around controller methods
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # Get instance of controller
            self = args[0];

            # Try to run controller method
            try:
                return fn(*args, **kwargs);
    
            # EarlyResponse is produced by a Flask view, contains template to be rendered
            except EarlyResponse as e:
                return e.response;
            
            # Catch any expected errors produced by controller or view
            except ValueError as e:
                return self.__view.display_error(e);
    
        return wrapper;
    
    """
    For flask only, set app created by entrypoint
    Flask framework requires app created by entrypoint to be visible to view that produces the web pages using the app
    """
    def set_flask_app(self, app):
        self.__view.set_app(app);
    
        
    # PART 1C
    # Allow the user to search the dataset by the local authority, showing the number of pupil enrolments in each local authority by time period (year).
    # – Given a list of local authorities, display in a well-formatted fashion the number of pupil enrolments in each local authority by time period (year).
    @catch_early_response 
    def get_enrolment_by_la_over_time(self,
        use_default = False,
    ):
        if use_default: 
            local_authorities = self.__defaults["la_name"];
        else:
            # Ask user for local authorities 
            self.responses = self.__view.prompt_user(
                prompts = ["Enter the local authorities you want to analyse"], 
                types = ["list"],
            );
            local_authorities = self.responses[0];
        
        frame = self.__absences.get_enrolment_by_la_over_time(
            local_authorities = local_authorities
        );
    
        self.__view.display_frame(frame);
    
    # PART 1D
    # Allow the user to search the dataset by school type, showing the total number of pupils who were given authorised absences in a specific time period (year).
    
    @catch_early_response
    def get_auth_by_school_type(self,
        use_default = False,
    ):
        if use_default:
            school_types = self.__defaults["school_type"];
            year = self.__defaults["time_period"];
    
        else:
            # Ask user for school types and year
            self.responses = self.__view.prompt_user(
                prompts = ["Enter the school types you want to analyse", "Enter the year you want to analyse"],
                types = ["list", "year"],
            );

            school_types = self.responses[0];
            year = self.responses[1];
        
        # Get and display required table
        frame = self.__absences.get_auth_by_school_type(
            school_types = school_types,
            years = [year]
        );
    
        self.__view.display_frame(frame);
    
    
    # Part 1D EXTENSION
    # Extend this by allowing the user to further see the breakdown of specific types of authorised absences given to pupils by school type in a specific time period (year).
    
    @catch_early_response
    def get_auth_by_school_type_detailed(self,
        use_default = False,
    ): 
        if use_default:
            school_types = self.__defaults["school_type"];
            year = self.__defaults["time_period"];
        
        else:
            # Get school types and year from user
            self.responses = self.__view.prompt_user(
                prompts = ["Enter the school types you want to analyse", "Enter the year you want to analyse"],
                types = ["list", "year"],
            );

            school_types = self.responses[0];
            year = self.responses[1];
    
        # Get and display required table
        frame = self.__absences.get_auth_by_school_type_detailed(
            school_types = school_types,
            years = [year]
        );
    
        self.__view.display_frame(frame);
    
    # PART 1E
    # Allow a user to search for all unauthorised absences in a certain year, broken down by either region name or local authority name.
    
    @catch_early_response
    def get_unauth_by_la_region(self,
        use_default = False,
    ):
        if use_default:
            year = self.__defaults["time_period"];
    
            # Randomly select region or local authority
            coin = random.randint(0, 1);
            if coin == 0:
                region_or_la = self.__defaults["region_name"];
            else:
                region_or_la = self.__defaults["la_name"];
    
        else:
            # Ask user for year and region or local authority
            self.responses = self.__view.prompt_user(
                prompts = ["Enter the year you want to analyse", "Enter the regions or local authorities you want to analyse"],
                types = ["year", "list"],
            );

            year = self.responses[0];
            region_or_la = self.responses[1];
        
        # Get and display required table
        frame = self.__absences.get_unauth_by_la_region(
            region_or_la = region_or_la,
            years = [year]
        );
        
        self.__view.display_frame(frame);
    
    # PART 2A
    # Allow a user to compare two local authorities of their choosing in a given year. Justify how you will compare and present the data.
    
    @catch_early_response
    def compare_la_in_year(self,
        use_default = False,
        cols = ["sess_authorised_percent", "sess_unauthorised_percent", "sess_overall_percent", "enrolments_pa_10_exact_percent", "sess_overall_percent_pa_10_exact"],
    ):
        if use_default:
            local_authorities = self.__defaults["la_name"];
            year = self.__defaults["time_period"];

        # Get local authorities and year from user
        else:
            self.responses = self.__view.prompt_user(
                prompts = ["Enter the local authorities you want to compare", "Enter the year you want to analyse"],
                types = ["list", "year"],
            );

            local_authorities = self.responses[0];
            year = self.responses[1];

        frame, datas = self.__absences.compare_la_in_year(
            local_authorities = local_authorities,
            years = [year],
            cols = cols 
        );
            
        self.__view.display_frame(frame);
        self.__view.display_graphs(datas,
            title = "Local authority comparison",
        );
    
    # PART 2B
    # Chart/explore the performance of regions in England from 2006-2018. Your charts and subsequent analysis in your report should answer the following questions:
    # – Are there any regions that have improved in pupil attendance over the years?
    # – Are there any regions that have worsened?
    # – Which is the overall best/worst region for pupil attendance?
    
    @catch_early_response
    def compare_region_attendance_over_time(self,
        data = "sess_overall_percent",
    ):
        frame, datas = self.__absences.compare_region_attendance_over_time(
            data = data
        );
        
        self.__view.display_frame(frame);
        self.__view.display_single_graph(datas,
            title = "Overall absence rate over time, by region",
        );
    
    # PART 3
    # Explore whether there is a link between school type, pupil absences and the location of the school. For example, is it more likely that schools of type X will have more pupil absences in location Y? Write the code that performs this analysis, and write a paragraph in your report (with appropriate visualisations/charts) that highlight + explain your findings.
    
    @catch_early_response
    def eda_school_type_location_absences(self):
        # Get data and display absences by school type
        school_type_absences_frame, school_type_absences_datas = self.__absences.get_school_type_absences();
        self.__view.display_frame(school_type_absences_frame);
        self.__view.display_single_graph(school_type_absences_datas,
            title = "Rates of overall absence, by school type",
            type = "bar",
            # Force confidence intervals to appear
            num_cols_for_cis = 0,
        );
        
        # Get data and display absences by region
        absences_region_frame, absences_region_datas = self.__absences.get_absences_region();
        self.__view.display_frame(absences_region_frame);
        self.__view.display_single_graph(absences_region_datas,
            title = "Rates of overall absence, by region",
            type = "bar",
            num_cols_for_cis = 0,
        );
    
        # Get data and display school types by region
        region_school_type_frame, region_school_type_datas = self.__absences.get_region_school_type();
        self.__view.display_frame(region_school_type_frame);
        self.__view.display_graphs(region_school_type_datas,
            title = "Proportion of school types, by region",
        );
    
    @catch_early_response
    def model_school_type_location_absences(self,
        display_results = False,
        display_detailed_results = False,
    ):
        # Get data and fit model
        frame = self.__absences.get_model_data();
        model = self.__absences.model_absences(frame = frame);
        
        # Display model summary
        if display_results or display_detailed_results:
            self.__view.display_line(model.summary);
        
        # Display full feature names, coefficient estimates and confidence intervals
        if display_detailed_results:
            # Extract coefficients and confidence intervals, and put on correct scale
            coefficients = self.__absences.scale_coefficients(model.coefficients);
            lower, upper = self.__absences.get_model_confidence_intervals(model, coefficients);
            
            # Extract feature names
            feature_names = self.__absences.get_feature_names(frame);

            [self.__view.display_line(
                f"feature name: {feature_names[i]}, coefficient: {coefficients[i]}, lower: {lower[i]}, upper: {upper[i]}"
            ) for i in range(len(feature_names))];

from absences import Absences;
from view import View;

import random;

"""
Class to handle the menu for the user to yeareract with the data
"""
class Menu:
    def __init__(self):
        # Instantiate data and view
        # Part 1a and 1b performed in constructor of Data class, initialised by Absences class
        self.__view = View();

        self.__view.display_line("Loading Spark... (this may take a while)");
        self.__absences = Absences(); 
        
        # Set default user inputs
        self.__defaults = self.__absences.get_default_values();
        
        # Start the menu loop
        self.__view.display_line("Loading complete!");
        self.__start_menu();
    
    """
    Start the menu loop giving the user options to interact with the data

    @param menu: dict of menu items the user can choose from
    """
    def __start_menu(self,
        menu = {
            "1": "Task 1C: Get enrolment, by local authority, over time",
            "2": "Task 1D: Get authorised absences, by school type, in a given year",
            "3": "Task 1D Extension: Get exact reasons for absence, by school type, in a given year",
            "4": "Task 1E: Get unauthorised absences, by local authority or region, in a given year",
            "5": "Task 2A: Chart and get various performance statistics, by local authorities, in a given year",
            "6": "Task 2B: Chart and get overall absence rates, by region, over time",
            "7": "Task 3, Part 1: Chart and get overall absences rates, region and school type",
            "8": "Task 3, Part 2: Model absences, by school type and region, and display results",
            "9": "Task 3, Part 3: Model absences, by school type and region, and display detailed results",
            "0": "Exit",
        },
    ):
        while True:
            # Show menu and ask user for choice
            self.__view.display_line("\nMAIN MENU");
            self.__view.display_menu(menu);
            
            choice = self.__view.prompt_user(
                prompt = "Enter your choice: ",
                type = "str"
            );
            
            try:
                if choice == "1": 
                    self.__get_enrolment_by_la_over_time();
                elif choice == "2":
                    self.__get_auth_by_school_type();
                elif choice == "3":
                    self.__get_auth_by_school_type_detailed();
                elif choice == "4":
                    self.__get_unauth_by_la_region();
                elif choice == "5":
                    self.__compare_la_in_year();
                elif choice == "6":
                    self.__compare_region_attendance_over_time();
                elif choice == "7":
                    self.__eda_school_type_location_absences();
                elif choice == "8":
                    self.__model_school_type_location_absences(display_results = True);
                elif choice == "9":
                    self.__model_school_type_location_absences(display_detailed_results = True);
                elif choice == "0":
                    self.__view.display_line("Goodbye!");
                    break;
                else:
                    raise Exception("Invalid choice.");

            except Exception as e:
                self.__view.display_line(e);

    # PART 1C
    # Allow the user to search the dataset by the local authority, showing the number of pupil enrolments in each local authority by time period (year).
    # – Given a list of local authorities, display in a well-formatted fashion the number of pupil enrolments in each local authority by time period (year).
    
    def __get_enrolment_by_la_over_time(self,
        use_default = False,
    ):
        if use_default: 
            local_authorities = self.__defaults["la_name"];
        else:
            # Ask user for local authorities 
            local_authorities = self.__view.prompt_user(
                prompt = "Enter the local authorities you want to analyse", 
                type = "list"
            );
        
        frame = self.__absences.get_enrolment_by_la_over_time(
            local_authorities = local_authorities
        );
    
        self.__view.display_frame(frame);
    
    # PART 1D
    # Allow the user to search the dataset by school type, showing the total number of pupils who were given authorised absences in a specific time period (year).
    
    def __get_auth_by_school_type(self,
        use_default = False,
    ):
        if use_default:
            school_types = self.__defaults["school_type"];
            year = self.__defaults["time_period"];
    
        else:
            # Ask user for school types 
            school_types = self.__view.prompt_user(
                prompt = "Enter the school types you want to analyse", 
                type = "list"
            );
                
            # Ask user for year
            year = self.__view.prompt_user(
                prompt = "Enter the year you want to analyse",
                type = "year"
            );
    
        # Get and display required table
        frame = self.__absences.get_auth_by_school_type(
            school_types = school_types,
            years = [year]
        );
    
        self.__view.display_frame(frame);
    
    
    # Part 1D EXTENSION
    # Extend this by allowing the user to further see the breakdown of specific types of authorised absences given to pupils by school type in a specific time period (year).
    
    def __get_auth_by_school_type_detailed(self,
        use_default = False,
    ): 
        if use_default:
            school_types = self.__defaults["school_type"];
            year = self.__defaults["time_period"];
        
        else:
            # Ask user for school types
            school_types = self.__view.prompt_user(
                prompt = "Enter the school types you want to analyse", 
                type = "list"
            );
    
            # Ask user for year
            year = self.__view.prompt_user(
                prompt = "Enter the year you want to analyse",
                type = "year"
            );
    
        # Get and display required table
        frame = self.__absences.get_auth_by_school_type_detailed(
            school_types = school_types,
            years = [year]
        );
    
        self.__view.display_frame(frame);
    
    # PART 1E
    # Allow a user to search for all unauthorised absences in a certain year, broken down by either region name or local authority name.
    
    def __get_unauth_by_la_region(self,
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
            # Ask user for year
            year = self.__view.prompt_user(
                prompt = "Enter the year you want to analyse",
                type = "year"
            );
    
            # Ask user for mixed region and local authority
            region_or_la = self.__view.prompt_user(
                prompt = "Enter the regions or local authorities you want to analyse",
                type = "list"
            );
        
        # Get and display required table
        frame = self.__absences.get_unauth_by_la_region(
            region_or_la = region_or_la,
            years = [year]
        );
        
        self.__view.display_frame(frame);
    
    # PART 2A
    # Allow a user to compare two local authorities of their choosing in a given year. Justify how you will compare and present the data.
    
    def __compare_la_in_year(self,
        use_default = False,
        cols = ["sess_authorised_percent", "sess_unauthorised_percent", "sess_overall_percent", "enrolments_pa_10_exact_percent", "sess_overall_percent_pa_10_exact"],
    ):
        if use_default:
            local_authorities = self.__defaults["la_name"];
            year = self.__defaults["time_period"];
    
        else:
            # Get local authorities from the user
            local_authorities = self.__view.prompt_user(
                prompt = "Enter the local authorities you want to compare", 
                type = "list"
            );
            
            # Get the year from the user
            year = self.__view.prompt_user(
                prompt = "Enter the year you want to analyse", 
                type = "year"
            );
        
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
    
    def __compare_region_attendance_over_time(self,
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
    
    def __eda_school_type_location_absences(self):
        # Get data and display absences by school type
        school_type_absences_frame, school_type_absences_datas = self.__absences.get_school_type_absences();
        self.__view.display_frame(school_type_absences_frame);
        self.__view.display_single_graph(school_type_absences_datas,
            title = "Rates of overall absence, by school type",
            type = "bar"
        );
        
        # Get data and display absences by region
        absences_region_frame, absences_region_datas = self.__absences.get_absences_region();
        self.__view.display_frame(absences_region_frame);
        self.__view.display_single_graph(absences_region_datas,
            title = "Rates of overall absence, by region",
            type = "bar"
        );
    
        # Get data and display school types by region
        region_school_type_frame, region_school_type_datas = self.__absences.get_region_school_type();
        self.__view.display_frame(region_school_type_frame);
        self.__view.display_graphs(region_school_type_datas,
            title = "Proportion of school types, by region",
        );
    
    def __model_school_type_location_absences(self,
        display_results = False,
        display_detailed_results = False,
    ):
        # Get data and names of covariates
        frame = self.__absences.get_model_data();
        feature_names = self.__absences.get_feature_names(frame);
        
        # Fit model
        model = self.__absences.model_absences(frame = frame);
        
        # Extract coefficients and confidence intervals, and put on correct scale
        coefficients = self.__absences.scale_coefficients(model.coefficients);
        lower, upper = self.__absences.get_model_confidence_intervals(model, coefficients);
        
        # Display model summary
        if display_results or display_detailed_results:
            self.__view.display_line(model.summary);
        
        # Display full feature names, coefficient estimates and confidence intervals
        if display_detailed_results:
            [self.__view.display_line({
                "feature": feature_names[i],
                "coefficient": coefficients[i],
                "lower": lower[i],
                "upper": upper[i]
            }) for i in range(len(feature_names))];

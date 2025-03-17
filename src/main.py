from absences import Absences;
from view import View;

import random;

# Instantiate data and view
# Part 1a and 1b performed in constructor of Data class, initialised by Absences class
print("Loading data...");
absences = Absences(); 

print("Loading graphics...");
view = View();

# Set default user inputs
defaults = absences.get_default_values();
print(defaults);
print("Defaults set!");

print("Loading complete!");

# PART 1C
# Allow the user to search the dataset by the local authority, showing the number of pupil enrolments in each local authority by time period (year).
# – Given a list of local authorities, display in a well-formatted fashion the number of pupil enrolments in each local authority by time period (year).

def get_enrolment_by_la_over_time(
    use_default = True,
):
    if use_default: 
        local_authorities = defaults["la_name"];
    else:
        # Ask user for local authorities 
        local_authorities = view.prompt_user(
            prompt = "Enter the local authorities you want to analyse", 
            type = "list"
        );
    
    frame = absences.get_by_la(
        local_authorities = local_authorities
    );

    view.display_frame(frame);

# PART 1D
# Allow the user to search the dataset by school type, showing the total number of pupils who were given authorised absences in a specific time period (year).

def get_auth_by_school_type(
    use_default = True,
):
    if use_default:
        school_types = defaults["school_type"];
        year = defaults["time_period"];

    else:
        # Ask user for school types 
        school_types = view.prompt_user(
            prompt = "Enter the school types you want to analyse", 
            type = "list"
        );
            
        # Ask user for year
        year = view.prompt_user(
            prompt = "Enter the year you want to analyse",
            type = "int"
        );

    frame = absences.get_auth_by_school_type(
        school_types = school_types,
        years = [year]
    );

    view.display_frame(frame);

# Part 1D EXTENSION
# Extend this by allowing the user to further see the breakdown of specific types of authorised absences given to pupils by school type in a specific time period (year).

def get_auth_by_school_type_detailed(
    use_default = True,
): 
    if use_default:
        school_types = defaults["school_type"];
        year = defaults["time_period"];
    
    else:
        # Ask user for school types
        school_types = view.prompt_user(
            prompt = "Enter the school types you want to analyse", 
            type = "list"
        );

        # Ask user for year
        year = view.prompt_user(
            prompt = "Enter the year you want to analyse",
            type = "int"
        );

    frame = absences.get_auth_by_school_type_detailed(
        school_types = school_types,
        years = [year]
    );

    view.display_frame(frame);


# PART 1E
# Allow a user to search for all unauthorised absences in a certain year, broken down by either region name or local authority name.

def get_unauth_by_la_region(
    use_default = True,
):
    if use_default:
        year = defaults["time_period"];

        # Randomly select region or local authority
        coin = random.randint(0, 1);
        if coin == 0:
            region_or_la = defaults["region_name"];
        else:
            region_or_la = defaults["la_name"];

    else:
        # Ask user for year
        year = view.prompt_user(
            prompt = "Enter the year you want to analyse",
            type = "int"
        );

        # Ask user for mixed region and local authority
        region_or_la = view.prompt_user(
            prompt = "Enter the regions or local authorities you want to analyse",
            type = "list"
        );
    
    frame = absences.get_unauth_by_la_region(
        region_or_la = region_or_la,
        years = [year]
    );

    view.display_frame(frame);

# PART 2A
# Allow a user to compare two local authorities of their choosing in a given year. Justify how you will compare and present the data.

def compare_la_in_year(
    use_default = True,
    cols = ["sess_authorised_percent", "sess_unauthorised_percent", "sess_overall_percent", "sess_authorised_percent_pa_10_exact", "sess_unauthorised_percent_pa_10_exact", "sess_overall_percent_pa_10_exact"],
):
    if use_default:
        local_authorities = defaults["la_name"];
        year = defaults["time_period"];

    else:
        # Get local authorities from the user
        local_authorities = view.prompt_user(
            prompt = "Enter the local authorities you want to compare", 
            type = "list"
        );
        
        # Get the year from the user
        year = view.prompt_user(
            prompt = "Enter the year you want to analyse", 
            type = "int"
        );
    
    frame = absences.compare_la_in_year(
        local_authorities = local_authorities,
        years = [year],
        cols = cols 
    );

    view.display_frame(frame);
    return frame;

# TODO analyse data

# PART 2B
# Chart/explore the performance of regions in England from 2006-2018. Your charts and subsequent analysis in your report should answer the following questions:
# – Are there any regions that have improved in pupil attendance over the years?
# – Are there any regions that have worsened?
# – Which is the overall best/worst region for pupil attendance?

def compare_region_attendance_over_time(
    data = "sess_overall_percent"
):
    frame = absences.compare_region_attendance_over_time(
        data = data
    );

    view.display_frame(frame);
    return frame;

# TODO chart and analyse data

# PART 3
# Explore whether there is a link between school type, pupil absences and the location of the school. For example, is it more likely that schools of type X will have more pupil absences in location Y? Write the code that performs this analysis, and write a paragraph in your report (with appropriate visualisations/charts) that highlight + explain your findings.

def analyse_school_type_location_absences(
    response = "sess_overall",
    covariates = ["school_type", "region_name", "academy_type", "academy_open_date", "all_through", "time_period", "sess_unauthorised_percent"],
    offset = "enrolments"
):
    frame = absences.analyse_school_type_location_absences(
        response = response, 
        covariates = covariates,
        offset = offset
    );

    view.display_frame(frame);
    return frame;

frame = analyse_school_type_location_absences();

# TODO chart and analyse data

from absences import Absences;
from view import View;

# Instantiate data and view
# Part 1a and 1b performed in constructor of Data class, initialised by Absences class
print("Loading data...");
absences = Absences(); 

print("Loading graphics...");
view = View();

print("Loading complete!");

# PART 1C
# Allow the user to search the dataset by the local authority, showing the number of pupil enrolments in each local authority by time period (year).
# – Given a list of local authorities, display in a well-formatted fashion the number of pupil enrolments in each local authority by time period (year).

def get_by_la(
    use_default = True,
    default_local_authorities = ["Sheffield", "Barnsley", "Rotherham", "Doncaster"]
):
    if use_default: 
        local_authorities = default_local_authorities;
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

def get_by_school_type(
    use_default = True,
    default_school_types = ["State-funded primary", "State-funded secondary", "Special", "Total"],
    default_year = "201819"
):
    if use_default:
        school_types = default_school_types;
        year = default_year;

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

    frame = absences.get_by_school_type(
        school_types = school_types,
        years = [year]
    );

    view.display_frame(frame);

# Part 1D EXTENSION
# Extend this by allowing the user to further see the breakdown of specific types of authorised absences given to pupils by school type in a specific time period (year).

def get_by_school_type_detailed(
    use_default = True,
    default_school_types = ["State-funded primary", "State-funded secondary", "Special", "Total"],
    default_year = "201819"
): 
    if use_default:
        school_types = default_school_types;
        year = default_year;
    
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

    frame = absences.get_by_school_type_detailed(
        school_types = school_types,
        years = [year]
    );

    view.display_frame(frame);


# PART 1E
# Allow a user to search for all unauthorised absences in a certain year, broken down by either region name or local authority name.

def get_unauthorised_absences(
    use_default = True,
    default_year = "201819",
    default_region_or_la = ["North East", "Hartlepool"]
):
    if use_default:
        year = default_year;
    
    else:
        # Ask user for year
        year = view.prompt_user(
            prompt = "Enter the year you want to analyse",
            type = "int"
        );

    # Ask user for region or local authority
    region_or_la = view.prompt_user(
        prompt = "Enter the regions and/or local authorities you want to analyse",
        type = "list"
    );
    
    frame = absences.get_unauthorised_absences(
        region_or_la = region_or_la,
        years = [year]
    );

    view.display_frame(frame);

get_unauthorised_absences();

# PART 2A
# Allow a user to compare two local authorities of their choosing in a given year. Justify how you will compare and present the data.

"""
Compare two local authorities in a given year
@param datas: list of data labels to compare by
"""
def get_by_la_year(
    datas = ["sess_authorised_percent", "sess_unauthorised_percent", "sess_overall_percent", "sess_authorised_percent_pa_10_exact", "sess_unauthorised_percent_pa_10_exact", "sess_overall_percent_pa_10_exact"],
):
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
 
frame = get_by_la_year();
view.display_frame(frame);

# TODO analyse data

# PART 2B
# Chart/explore the performance of regions in England from 2006-2018. Your charts and subsequent analysis in your report should answer the following questions:
# – Are there any regions that have improved in pupil attendance over the years?
# – Are there any regions that have worsened?
# – Which is the overall best/worst region for pupil attendance?

# Get data on regions and attendance
data = absences.get_data(
    data = "attendance", 
    row = "region", 
    col = "year_breakdown"
);

# TODO chart and analyse data

# PART 3
# Explore whether there is a link between school type, pupil absences and the location of the school. For example, is it more likely that schools of type X will have more pupil absences in location Y? Write the code that performs this analysis, and write a paragraph in your report (with appropriate visualisations/charts) that highlight + explain your findings.

# Get data on school type and location
data = absences.get_data(
    data = "sess_overall", 
    row = "school_type", 
    col = "region_name"
);

# TODO chart and analyse data

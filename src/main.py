from data import Data;
from view import View;

# Instantiate data and view
# Part 1a and 1b performed in constructor of data
absences = Data(); 
view = View();

# PART 1D
# Allow the user to search the dataset by the local authority, showing the number of pupil enrolments in each local authority by time period (year).
# – Given a list of local authorities, display in a well-formatted fashion the number of pupil enrolments in each local authority by time period (year).

"""
Get number of pupil enrolments for requested local authorities
"""
def get_by_la():
    # Ask user for local authorities 
    local_authorities = view.prompt_user(
        prompt = "Enter the local authorities you want to analyse", 
        type = "list"
    );
    
    # Get enrolment data for the requested local authorities
    frame = absences.get_agg_frame(
        data = "enrolments", 
        row = "la_name", 
        selected_rows = local_authorities,
    );

    return frame;

# PART 1D
# Allow the user to search the dataset by school type, showing the total number of pupils who were given authorised absences in a specific time period (year).
"""
Get authorised absence data for requested school types
"""
def get_by_school_type():
    # Ask user for school types 
    school_types = view.prompt_user(
        prompt = "Enter the school types you want to analyse", 
        type = "list"
    );
    
    # Get authorised absence data for the school types
    frame = absences.get_agg_frame(
        data = "sess_authorised", 
        row = "school_type", 
        selected_rows = school_types,
    );

    return frame;

# Part 1D EXTENSION
# Extend this by allowing the user to further see the breakdown of specific types of authorised absences given to pupils by school type in a specific time period (year).
"""
Get authorised absence data by absence reasons for requested school types
"""
def get_by_absence_reasons(
    authorised_prefix = "sess_auth_",
    title_col = "school_type",
    datas_category = "absence_reasons"
):
    # Ask user for school types
    school_types = view.prompt_user(
        prompt = "Enter the school types you want to analyse", 
        type = "list"
    );

    # Get every absence reason 
    absence_reasons = absences.get_absence_reasons();
    
    # Get one frame per school type detailing authorised absences over time by absence reason
    frames = absences.get_batch_multi_agg_frames(
        title_col = title_col,
        titles = school_types,
        datas_category = datas_category,
        datas = absence_reasons,
        col_prefix = authorised_prefix
    );

    return frames;

# PART 1E
# Allow a user to search for all unauthorised absences in a certain year, broken down by either region name or local authority name.

"""
Get unauthorised absences in a certain year, by region name and local authority name
"""
def get_unauthorised_absences():
    # Create dictionary of both region and local authority data
    frames = absences.get_batch_agg_frames(
        datas = ["sess_unauth_totalreasons", "sess_unauth_totalreasons"],
        rows = ["region_name", "la_name"],
        titles = ["Region", "Local Authority"]
    );

    return frames;

frames = get_unauthorised_absences();
view.display_multiple_frames(frames);

# PART 2A
# Allow a user to compare two local authorities of their choosing in a given year. Justify how you will compare and present the data.

"""
Get authorised and unauthorised absence data by school type for requested local authorities in a given year
"""
def get_by_la_year():
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
    
    # Specify the columns to be analysed
    columns = ["school_type", "authorised_absences", "unauthorised_absences"];
    

data = get_by_la_year();

# Display the data
view.display_data(data);

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

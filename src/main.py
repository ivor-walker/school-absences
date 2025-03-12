from data import Data;
from view import View;

# Instantiate data and view
# Part 1a and 1b performed in constructor of data
absences = Data(); 
view = View();

# Part 1c
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
    data = absences.get_agg_frame(
        data = "enrolments", 
        row = "la_name", 
        selected_rows = local_authorities,
    );

    return data;

view.display_data(get_by_la());

# Part 1d
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
    data = absences.get_agg_frame(
        data = "authorised_absences", 
        row = "school_type", 
        selected_rows = school_types,
        col = "year_breakdown",
    );

    return data;

view.display_data(get_by_school_type());

# Part 1d extension

"""
Get authorised absence data by absence reasons for requested school types
"""
def get_by_absence_reasons():
    # Ask user for school types
    school_types = view.prompt_user(
        prompt = "Enter the school types you want to analyse", 
        type = "list"
    );

    # Get every absence reason 
    absence_reasons = absences.get_absence_reasons();
    
    # Get authorised absence data for the absence reasons
    data = data.get_batch_agg_frames(
        title_col = "school_type",
        titles = school_types,
        datas = absence_reasons,
    );

    return data;

data = get_by_absence_reasons();
[view.display_multiple_data(frame) for title, frame in data.items()]; 

# Part 2a
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
        prompt = "Enter the year you want to analyse: ", 
        type = "int"
    );
    
    # Specify the columns to be analysed
    columns = ["school_type", "authorised_absences", "unauthorised_absences"];
    
    # Get data specified in 'columns' for the local authorities
    data = absences.get_data(columns, local_authorities, columns,
        filter = f'la_name = {local_authorities} AND year = {year}',
    );

data = get_by_la_year();

# Display the data
view.display_data(data);

# TODO analyse data

# Part 2b

# Get data on regions and attendance
data = absences.get_data(
    data = "attendance", 
    row = "region", 
    col = "year_breakdown"
);

# TODO chart and analyse data

# Part 3

# Get data on school type and location
data = absences.get_data(
    data = "sess_overall", 
    row = "school_type", 
    col = "region_name"
);

# TODO chart and analyse data

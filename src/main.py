# For creating sparksession
from pyspark.sql import SparkSession;

"""
Class that produces data: loads the absences data and provides methods to extract information from it
"""
class AbsencesData:
    
    """ 
    Constructor for the AbsencesAnalysis class
    absences_loc: str, the location of the absences file
    """
    # Default location of the absences file
    absences_loc = "data/Absence_3term201819_nat_reg_la_sch.csv"; 

    def __init__(self, absences_loc = None):
        # If the absences location is provided, override the default location  
        if absences_loc:
            self.absences_loc = absences_loc;
        
        create_spark_session();
        load_data();

    """
    Create a SparkSession
    """
    def create_spark_session(self):
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("school-absences-analysis")
            .getOrCreate()
        );

    """
    Load the data from the absences file as an Apache dataframe, and set it to the data attribute
    """
    def load_data(self):
        self.data = ( 
            spark.read
            .option("inferSchema", "true")
            .option("header", "true")
            .csv(self.absences_loc)
        );

    """
    Helper method to produce dataframes of the data
    data_label: str, the label of the data to be extracted
    row_labels: str, the labels of the rows to be extracted
    col_labels: str, the labels of the columns to be extracted
    filter: str, the filter to be applied to the data
    """
    def get_data(self, data_label, row_labels, col_labels, filter = None):
        # TODO
        pass;

    """
    Helper method to get all reasons for absence
    """
    def get_absence_reasons(self):
        # TODO
        pass;


"""
Prompt the user for input
prompt: str, the prompt to display to the user
type: str, the type of the input to be expected
"""
def prompt_user(prompt, type = 'str'):
    # TODO
    pass;

"""
Display the data to the user
data: DataFrame, the data to be displayed
"""
def display_data(data):
    # TODO
    pass;

# Instantiate the AbsencesData class
# Part 1a and 1b performed in constructor
absences = AbsencesData();

# Part 1c
# Get local authorities from the user
local_authorities = prompt_user("Enter the local authorities you want to analyse, seperated by commas: ", type = 'list');

# Get enrolment data for the local authorities
data = absences.get_data("enrolments", "la_name", "year_breakdown",
        filter = f'la_name = {local_authorities}'
);

# Display the data
display_data(data);

# Part 1d
# Prompt the user for input
school_types = prompt_user("Enter the school types you want to analyse: ", type = 'list');

# Get authorised absence data for the school types
data = absences.get_data("authorised_absences", "school_type", "year_breakdown",
        filter = f'school_type = {school_types}'
);

# Display the data
display_data(data);

# Part 1d extension

# Get authorised absence data for each absence reason
absence_reasons = absences.get_absence_reasons();

# Get authorised absence data for each absence reason
data = absences.get_data("authorised_absences", absence_reasons, "year_breakdown",
        filter = f'school_type = {school_types}'
);

# Display the data
display_data(data);

# Part 2a

# Get local authorities from the user
local_authorities = prompt_user("Enter the local authorities you want to compare, seperated by commas: ", type = 'list');

# Get the year from the user
year = prompt_user("Enter the year you want to analyse: ", type = 'int');

# Specify the columns to be analysed
columns = ["school_type", "authorised_absences", "unauthorised_absences"];

# Get data specified in 'columns' for the local authorities
data = absences.get_data(columns, local_authorities, columns,
    filter = f'la_name = {local_authorities} AND year = {year}',
);

# Display the data
display_data(data);

# TODO analyse data

# Part 2b

# Get data on regions and attendance
data = absences.get_data("attendance", "region", "year_breakdown");

# TODO chart and analyse data

# Part 3

# Get data on school type and location
data = absences.get_data("sess_overall", "school_type", "region_name");

# TODO chart and analyse data

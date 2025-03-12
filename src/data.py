from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col

"""
Class that produces data: loads the absences data and provides methods to extract information from it
"""
class Data:
    
    """ 
    Constructor: creates a SparkSession and load the data
    absences_loc: str, the location of the absences file
    """
    def __init__(self, 
        absences_loc = "data/Absence_3term201819_nat_reg_la_sch.csv"
    ):
        self.create_spark_session();

        self.load_data(csv_loc = absences_loc);
        
        self.get_case_mapping();

    """
    Create a SparkSession
    """
    def create_spark_session(self,
        master = "local[*]",
        app_name = "school-absences-analysis"
    ):
        self.spark = (
            SparkSession.builder
            .master(master)
            .appName(app_name)
            .getOrCreate()
        );

    """
    Load the data from the absences file as an Apache dataframe, and set it to the data attribute
    @param csv_loc: str, the location of data to load
    """
    def load_data(self,
        csv_loc = None
    ):
        self.data = ( 
            self.spark.read
            .option("inferSchema", "true")
            .option("header", "true")
            .csv(csv_loc)
        );
    
    """
    Get inferred case of values in each string column in the data
    """
    def get_case_mapping(self,
        n = 100,
        minor_words = ["and", "or", "the", "a", "an", "in", "on", "at", "to", "of", "for", "by", "with", "from"]
    ):
        self.minor_words = minor_words;
        self.case_mapping = {};
        
        string_cols = [col for col in self.data.columns if str(self.data.schema[col].dataType) == "StringType()"];
        for col in string_cols:
            # Get subset of data with non-null values
            subset = self.data.filter(col(col).isNotNull()).select(col).limit(n).collect();

            # Get inferred cases of values in column
            inferred_cases = [self.infer_case(row[col]) for row in subset];

            # Get least common case
            case = min(set(inferred_cases), key = inferred_cases.count);

            self.case_mapping[col] = case;

        breakpoint();

    """
    Infer case of a string
    @param string: str, the string to infer the case of
    @param minor_words: list of str, words that should be lowercase during proper case
    """
    def infer_case(self,
        string = None,
    ):

        # Entirely lower or upper case
        if string.islower():
            return "lower";
        elif string.isupper():
            return "upper";
        
        # Title case
        elif string.istitle():
            return "title";

        # Sentence case
        elif string[0].isupper():
            return "sentence";

        words = string.split(" ");
        
        # Seperate words into major and minor
        minor_words = self.minor_words;
        found_major_words = [word for word in words if word.lower() not in minor_words];
        found_minor_words = [word for word in words if word.lower() in minor_words];

        # If all major words are title case and all minor words are lower case, then proper case
        if all([word.istitle() for word in found_major_words]) and all([word.islower() for word in found_minor_words]):
            return "proper";

        # If case cannot be inferred, throw error
        raise ValueError(f"Case of '{string}' could not be inferred!");
    
    """
    Convert a string to a specified case
    @param string: str, the string to convert
    @param case: str, the case to convert the string to
    """
    def convert_case(self,
        string = None,
        case = None
    ):
        if case == "lower":
            return string.lower();
        elif case == "upper":
            return string.upper();

        elif case == "title":
            return string.title();

        elif case == "sentence":
            return string[0].upper() + string[1:].lower();

        elif case == "proper":
            words = string.split(" ");
            words = [word.title() if word.lower() not in self.minor_words else word.lower() for word in words];
            return " ".join(words);
        else:
            raise ValueError(f"Case '{case}' not recognised!");

    """
    Produce dataframe of aggregates of data, by all values of a single column and row label
    @param data: str, label of data to aggregate
    @param row: str, label of row values to use
    @param selected_rows: list of str, 
    @param col: str, label of column values to use (default year)
    """
    def get_agg_frame(self, 
        data = None, 
        row = None, 
        selected_rows = None,
        col = "time_period", 
    ):
        # Get frame, row and col frame as columns from original frame 
        requested_cols = [data, row, col];
        frame = self.data.select(requested_cols);
        
        # Remove rows with missing frame
        frame = frame.dropna();

        # Get requested rows only (if specified) 
        if selected_rows:
            frame = self.get_selected_rows(
                frame = frame,
                row = row,
                selected_rows = selected_rows
            );

        # Transpose frame
        frame = frame.groupBy(row).pivot(col).sum(data);

        return frame;
   
    """
    Get a subset of rows from a dataframe
    @param frame: dataframe, the dataframe to get the subset from
    @param row: str, label of row values to use
    @param selected_rows: list of str, the rows to select
    """
    def get_selected_rows(self,
        frame = None,
        row = None,
        selected_rows = None
    ):
        # Get case of row
        case = self.case_mapping[row];
        
        # Convert row to match case of row in frame
        selected_rows = [self.convert_case(row, case) for selected_row in selected_rows]; 

        # Get missing selected rows
        missing_selected_rows = [
            selected_row for selected_row in selected_rows 
            if frame.filter(col(row) == selected_row)
            # Since we are only checking for existence, we can limit to 1 row
            .limit(1)
            .count() == 0
        ];

        # Raise error if missing selected rows
        if len(missing_selected_rows) > 0:
            missing_selected_rows = "', '".join(missing_selected_rows);
            raise ValueError(f"Rows '{missing_selected_rows}' not found in {row}!");
        
        return frame;

    """
    Produce aggregates of data by all values of a single column label and a list of rows
    @param data: str, label of data values to use
    @param rows: list of str, labels of rows values to use
    @param col: str, label of column values to use
    @param filter: str, filter to apply to the data
    """
    def get_table_multi_rows(self, 
        data = None, 
        rows = None, 
        col = None, 
        filter = None
    ):
        # Get data, all specified rows and col data as columns from original data
        data = self.data.select(data, rows, col);

        # Apply filter
        if filter:
            data = data.filter(filter);
        
        breakpoint();
        # Transpose data
        data = data.groupBy(rows).pivot(col).sum(data);

        return data;

    """
    Get all column names which are reasons for absence
    """
    def get_absence_reasons(self):
        return [col for col in self.data.columns if is_reason_for_absence(col)];         

    """
    Helper method to check if column is a reason for absence
    """
    def is_reason_for_absence(self, col):
        return "auth" in col and "unauth" not in col;

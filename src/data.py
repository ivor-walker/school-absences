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
        n = 50,
        minor_words = ["and", "or", "the", "a", "an", "in", "on", "at", "to", "of", "for", "by", "with", "from", "upon"]
    ):
        self.minor_words = minor_words;
        self.case_mapping = {};
        
        # Get all columns with a string data type
        string_cols = [col for col in self.data.columns if str(self.data.schema[col].dataType) == "StringType()"];
        
        for string_col in string_cols:
            # Get subset of data with non-null values
            subset = self.data.filter(col(string_col).isNotNull()).select(string_col).limit(n).collect();

            # Get inferred cases of values in string_column
            try:
                inferred_cases = [self.infer_case(row[string_col]) for row in subset];
            except ValueError as e:
                print(e);
            
            # Set to proper case if any value is proper case
            if "proper" in inferred_cases:
                case = "proper";

            # Set to sentence case if any value is sentence case
            elif "sentence" in inferred_cases:
                case = "sentence";

            # Else, set to most common case
            else:
                case = max(set(inferred_cases), key = inferred_cases.count);

            self.case_mapping[string_col] = case;


    """
    Infer case of a string
    @param string: str, the string to infer the case of
    @param minor_words: list of str, words that should be lowercase during proper case
    """
    def infer_case(self,
        string = None,
    ):
        # Trim string
        string = string.strip();

        # Not composed of words 
        if string.count("/") == 2:
            return "date";
        if string.isnumeric():
            return "numeric";
        if not any([char.isalpha() for char in string]):
            return "symbol";

        # Entirely lower or upper case
        if string.islower():
            return "lower";
        if string.isupper():
            return "upper";
        
        words = string.split(" ");
        
        # Cases with one word
        if len(words) == 1:
            
            # Title case        
            if self.is_title(string):
                return "title";
        
        # Cases with multiple words
        else:
            # Sentence case: more than one word, first word is title case, and all other words are lower case
            if self.is_title(words[0]) and all([word.islower() for word in words[1:]]):
                return "sentence";
        
            # Seperate words into major and minor
            minor_words = self.minor_words;
            found_major_words = [word for word in words if word.lower() not in minor_words];
            found_minor_words = [word for word in words if word.lower() in minor_words];

            # If all major words are title case and all minor words are lower case, then proper case
            if all([self.is_title(word) for word in found_major_words]) and all([word.islower() for word in found_minor_words]):
                return "proper";
        
        # If case cannot be inferred, throw error
        raise ValueError(f"Case of '{string}' could not be inferred!");
    
    """
    Helper method to check if string is title
    @param string: str, the string to check
    """
    def is_title(self, string):
        return string[0].isupper();

    """
    Helper method to convert string to title
    @param string: str, the string to convert
    """
    def to_title(self, string):
        return string[0].upper() + string[1:];

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
            return self.to_title(string);

        elif case == "sentence":
            words = string.split(" ");
            words = [self.to_title(word) if index == 0 else word.lower() for index, word in enumerate(words)];
            return " ".join(words);

        elif case == "proper":
            words = string.split(" ");
            words = [word.title() if word.lower() not in self.minor_words else word.lower() for word in words];
            return " ".join(words);

        else:
            return string;

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
        selected_rows = [self.convert_case(selected_row, case) for selected_row in selected_rows]; 

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
        
        # Filter frame to only include selected rows
        frame = frame.filter(
            col(row)
            .isin(selected_rows)
        );

        return frame;

    """
    Produces multiple aggregated frames, each showing a different selected row value 
    """
    def get_batch_agg_frames(self,
        title_col = None,
        titles = None,
        datas = None,
        col = "time_period"
    ):
        breakpoint();

        frames = {
            title: self.get_multi_col_agg_frame(
                title_col = title_col,
                title = title,
                rows = datas, 
                col = col
            )
            for index, title in enumerate(titles)
        };

        return frames;
    
    """
    Get a frame containing multiple rows of different data
    """
    def get_multi_col_agg_frame(self,
        title_col = None,
        title = None,
        rows = None,
        col = None,
    ):
        breakpoint();

        # Get frame, row and col frame as columns from original frame 
        requested_cols = rows + [col];
        frame = self.data.select(requested_cols);
        
        # Remove rows with missing frame
        frame = frame.dropna();
        
        # Remove rows not belonging to title
        title_col_case = self.case_mapping[title_col];
        title = self.convert_case(title, title_col_case);

        frame = frame.filter(col(title_col) == title);
        
        # Transpose frame
        frame = frame.groupBy(rows).pivot(col).sum(rows);
        
        breakpoint();

        return frame;

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

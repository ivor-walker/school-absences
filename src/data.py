from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

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
        self.__create_spark_session();

        self.__load_data(csv_loc = absences_loc);
        
        self.__get_case_mapping();

    """
    Create a SparkSession
    """
    def __create_spark_session(self,
        master = "local[*]",
        app_name = "school-absences-analysis"
    ):
        self.__spark = (
            SparkSession.builder
            .master(master)
            .appName(app_name)
            .getOrCreate()
        );

    """
    Load the data from the absences file as an Apache dataframe, and set it to the data attribute
    @param csv_loc: str, the location of data to load
    """
    def __load_data(self,
        csv_loc = None
    ):
        self.__data = ( 
            self.__spark.read
            .option("inferSchema", "true")
            .option("header", "true")
            .csv(csv_loc)
        );
    
    """
    Get inferred case of values in each string column in the data
    """
    def __get_case_mapping(self,
        n = 50,
        minor_words = ["and", "or", "the", "a", "an", "in", "on", "at", "to", "of", "for", "by", "with", "from", "upon"]
    ):
        self.__minor_words = minor_words;
        self.__case_mapping = {};
        
        # Get all columns with a string data type
        string_cols = [col for col in self.__data.columns if str(self.__data.schema[col].dataType) == "StringType()"];
        
        for string_col in string_cols:
            # Get subset of data with non-null values
            subset = self.__data.filter(col(string_col).isNotNull()).select(string_col).limit(n).collect();

            # Get inferred cases of values in string_column
            inferred_cases = [self.__infer_case(row[string_col]) for row in subset];
            
            # Set to proper case if any value is proper case
            if "proper" in inferred_cases:
                case = "proper";

            # Set to sentence case if any value is sentence case
            elif "sentence" in inferred_cases:
                case = "sentence";

            # Else, set to most common case
            else:
                case = max(set(inferred_cases), key = inferred_cases.count);
            
            # Convert non-word values to corresponding data types 
            if case in ["date", "numeric", "symbol"]:
                self.__convert_str_col(string_col = string_col, case = case);

            self.__case_mapping[string_col] = case;

    """
    Infer case of a string
    @param string: str, the string to infer the case of
    """
    def __infer_case(self,
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
            if self.__is_title(string):
                return "title";
        
        # Cases with multiple words
        else:
            # Sentence case: more than one word, first word is title case, and all other words are lower case
            if self.__is_title(words[0]) and all([word.islower() for word in words[1:]]):
                return "sentence";
        
            # Seperate words into major and minor
            minor_words = self.__minor_words;
            found_major_words = [word for word in words if word.lower() not in minor_words];
            found_minor_words = [word for word in words if word.lower() in minor_words];

            # If all major words are title case and all minor words are lower case, then proper case
            if all([self.__is_title(word) for word in found_major_words]) and all([word.islower() for word in found_minor_words]):
                return "proper";
        
        # If case cannot be inferred, throw error
        raise ValueError(f"Case of '{string}' could not be inferred!");
    
    """
    Helper method to check if string is title
    @param string: str, the string to check
    """
    def __is_title(self, string):
        return string[0].isupper();

    """
    Helper method to convert string to title
    @param string: str, the string to convert
    """
    def __to_title(self, string):
        return string[0].upper() + string[1:];

    """
    Convert a string to a specified case
    @param string: str, the string to convert
    @param case: str, the case to convert the string to
    """
    def __convert_case(self,
        string = None,
        case = None
    ):
        if case == "lower":
            return string.lower();
        elif case == "upper":
            return string.upper();

        elif case == "title":
            return self.__to_title(string);

        elif case == "sentence":
            words = string.split(" ");
            words = [self.__to_title(word) if index == 0 else word.lower() for index, word in enumerate(words)];
            return " ".join(words);

        elif case == "proper":
            words = string.split(" ");
            words = [word.title() if word.lower() not in self.__minor_words else word.lower() for word in words];
            return " ".join(words);

        else:
            return string;
    
    """
    Convert all non-word string columns to non-string columns
    """
    def __convert_str_col(self,
        string_col = None,
        case = None
    ):
        # Date: cast to SQL date
        if case == "date":
            self.__data = self.__data.withColumn(string_col, col(string_col).cast("date"));
        
        # Numeric: cast to int
        elif case == "numeric":
            self.__data = self.__data.withColumn(string_col, col(string_col).cast("int"));
        
        # Symbol: convert all symbols to 0, then cast to int
        elif case == "symbol":
            self.__data = self.__data.withColumn(
                string_col, 
                regexp_replace(
                    col(string_col),
                    "[^0-9]", "0"
                ).cast("int")
            );

    """
    Produce dataframe of aggregates of data, by all values of a single column and row label
    @param data: str, label of data to aggregate
    @param row: str, label of row values to use
    @param selected_rows: list of str, 
    @param col: str, label of column values to use (default year)
    """
    def get_agg_frame(self, 
        data = None, 
        rows = [],
        selected_rows = [],
        col = "time_period"
    ):
        requested_cols = [data, row, col];
        frame = self.get_frame(
            requested_cols = requested_cols,
            rows = rows,
            selected_rows = selected_rows
        ); 

        # Transpose frame
        frame = self.__get_grouped_frame(
            frame = frame,
            group_by = [row],
            pivot = col,
            sum = data
        );

        return frame;
    
    """
    Get multiple aggregated frames containing one type of data
    @param datas: list of str, the datas to aggregate
    @param rows: list of str, rows of frames 
    @param cols: list of str, cols of frames
    @param titles: list of str, titles of frames
    @param default_col: str, default column
    """

    def get_batch_agg_frames(self,
        datas = None,
        rows = None,
        cols = None,
        titles = None,
        default_col = "time_period"
    ):
        if cols is None:
            cols = [default_col] * len(datas);

        frames = {};

        for index, data in enumerate(datas):
            # Get frame for data
            frame = self.get_agg_frame(
                data = data,
                row = rows[index],
                col = cols[index]
            );

            # Assign frame to frames dictionary
            frames[titles[index]] = frame;

        return frames;

    """
    Get a subframe with given columns
    @param frame: dataframe, the dataframe to get the subset from
    @param requested_cols: list of str, the columns to select
    @param rows: list of str, the row labels to check
    @param selected_rows: list of str, the selected rows to filter by
    @param or_and: str, the operator to use when filtering by selected rows
    """
    def get_frame(self,
        frame = None,
        requested_cols = None,
        rows = [],
        selected_rows = [],
        or_and = "and"
    ):
        if frame is None:
            frame = self.__data;
        
        if requested_cols is None:
            requested_cols = frame.columns;

        # Create filter query
        filter_query = self.__get_selected_rows(
            frame = frame,
            rows = rows,
            selected_rows = selected_rows,
            or_and = or_and
        );
        
        # Select requested columns and filter by selected rows
        frame = frame.select(requested_cols).filter(filter_query);
        
        # Remove rows with missing data
        frame = frame.dropna();

        return frame;

    """
    Generate a filter query to select rows from a frame 
    @param frame: dataframe, the dataframe to get the subset from
    @param rows: list of str, the column labels to get values from
    @param selected_rows: list of str, the values in rows to select
    @param or_and: str, the operator to use when filtering by selected rows
    """
    def __get_selected_rows(self,
        frame = None,
        rows = None,
        selected_rows = None,
        or_and = None
    ):
        # Convert selected rows to correct case and ensure they exist in frame
        for index, row in enumerate(rows):
            rows_to_select = selected_rows[index];

            # Get selected rows to conform to case of row in frame
            case = self.__case_mapping[row];
            selected_rows[index] = [self.__convert_case(selected_row, case) for selected_row in selected_rows]; 

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
        queries = [f"{row} == '{selected_row}'" for selected_row in selected_rows]; 
        query = f" {or_and} ".join(queries);
        
        return query;
    """
    Group by, pivot and sum over a frame
    """
    def __get_grouped_frame(self,
        frame = None,
        group_by = None,
        pivot = None,
        sum = None
    ):
        return frame.groupBy(group_by).pivot(pivot).sum(sum);

    """
    Produces multiple aggregated frames containing multiple types of data
    @param title_col: str, the column to use as the title
    @param titles: list of str, the titles to use
    @param datas_category: str, overall category of all datas to aggregate
    @param datas: list of str, the datas to aggregate
    @param col: str, the column to use as the column of each frame
    @param col_prefix: str, the prefix to remove from values of the column
    """
    def get_batch_multi_agg_frames(self,
        title_col = None,
        titles = None,
        datas_category = None,
        datas = None,
        col = "time_period",
        col_prefix = None
    ):
        frames = {};
        
        # Get initial frame containing all data
        initial_frame = self.get_frame(
            requested_cols = [title_col, col] + datas
        );
        
        for title in titles:
            # Get frame with only data for title
            frame = self.get_frame(
                frame = initial_frame,
                rows = [title_col],
                selected_rows = [title]
            );

            # Remove title column
            frame = frame.drop(title_col);

            # Transpose frame
            frame = self.get_multi_col_agg_frame(
                frame = frame,
                datas_category = datas_category,
                datas = datas,
                col = col
            );
            
            # Remove authorised prefix from values of data category column
            frame = frame.withColumn(
                datas_category, 
                regexp_replace(datas_category, col_prefix, "")
            ); 

            # Assign frame to frames dictionary
            frames[title] = frame;

        return frames;
    
    
    """
    Turn an initial frame into a frame of multiple aggregates  
    """
    def get_multi_col_agg_frame(self,
        frame = None,
        datas_category = None,
        datas = None,
        col = None
    ):
        # Create a stack expression
        n_datas = len(datas);
        stack_expr = f"""
            stack(
                {n_datas},
                {", ".join([f"'{data}', {data}" for data in datas])}
            ) as ({datas_category}, count)
        """;

        # Unpivot frame by stacking data columns
        frame = frame.selectExpr(col, stack_expr);

        # Group by and pivot frame
        frame = self.__get_grouped_frame(
            frame = frame,
            group_by = [datas_category],
            pivot = col,
            sum = "count"
        );
        
        # Sort data by last column
        last_col = frame.columns[-1];
        frame = frame.orderBy(frame[last_col].asc());
        
        return frame;

    """
    Get all column names which are reasons for absence
    @return list of str, the names of the columns which are reasons for absence
    """
    def get_absence_reasons(self):
        return [col for col in self.__data.columns if self.__is_reason_for_absence(col)];

    """
    Helper method to check if column is a reason for absence
    @param col: str, the column to check
    """
    def __is_reason_for_absence(self, col):
        return col.startswith("sess_auth_");

import os;

from pyspark.sql import SparkSession;
import pyspark.sql.functions as F;
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType;

import numpy as np;
from scipy.stats import t;

import re;

"""
Class to load and manipulate data with Spark
"""
class SparkData:
    
    """ 
    Constructor: creates a SparkSession, load data and perform preprocessing

    @param csv_loc: str, the location of csv data to load
    """
    def __init__(self, csv_loc):
        # Create SparkSession
        self.__create_spark_session();
        
        # Suppress logging
        self.__spark.sparkContext.setLogLevel("ERROR");
    
        # Load data as csv
        self.__load_csv(csv_loc);
        
        # Get cases of each column
        self.__get_case_mapping();

        # Round all numeric columns to 5 decimal places
        self.__round_numeric_cols();
        
    """
    Create a SparkSession

    @param master: str, the master to use (default is local mode)
    @param app_name: str, the name of the application
    @param python_path: str, the path to the python executable
    """
    def __create_spark_session(self,
        master = "local[*]",
        app_name = "school-absences-analysis",
        python_path = None,
    ):
        if python_path is not None:
            os.environ["PYSPARK_PYTHON"] = python_path;
            os.environ["PYSPARK_DRIVER_PYTHON"] = python_path;

        self.__spark = (
            SparkSession.builder
            .master(master)
            .appName(app_name)
            # Suppress console progress bar
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        );

    """
    Load the data from the absences file as an Apache dataframe, and set it to the data attribute

    @param csv_loc: str, the location of data to load
    """
    def __load_csv(self, csv_loc):
        self.__data = ( 
            self.__spark.read
            .option("inferSchema", "true")
            .option("header", "true")
            .csv(csv_loc)
        );
    
    """
    Getter of columns of dataframe

    @return cols: list of str, the columns of the dataframe
    """
    def _get_cols(self):
        return self.__data.columns;

    """
    Get first n non-NA rows from a column

    @param n: int, the number of rows to get

    @return values: list of Row, the first n non-NA rows from the column
    """
    def _get_first_values(self,
        n = 5,
    ):
        return self.__data.dropna().limit(n).collect();
    
    """
    Get list of all distinct non_NA values in a column

    @param col: str, the column to get distinct values from

    @return distinct_values: list of Row, the distinct non-NA values in the column
    """
    def _get_distinct_values(self, col):
        return self.__data.select(col).dropna().distinct().collect();
    
    """
    Rename columns in a DataFrame

    @param frame: DataFrame, the DataFrame to rename columns in
    @param col_renames: dict of str, the columns to rename and their new names

    @return DataFrame, the DataFrame with renamed columns
    """
    def _rename_cols(self,
        frame = None,
        col_renames = {}
    ):
        if frame is None:
            frame = self.__data

        for col in col_renames:
            frame = frame.withColumnRenamed(col, col_renames[col]);

        return frame;
    
    """
    Transform and replace values in a column

    @param frame: DataFrame, the DataFrame to transform values in
    @param col: str, the column to transform
    @param query: str, SQL expression to transform values

    @return DataFrame, the DataFrame with transformed values
    """
    def _transform_col(self,
        frame = None,
        col = None,
        query = None,
        temp_name = "transform_frame"
    ):
        if frame is None:
            frame = self.__data;
        
        # Create temporary view of frame
        frame.createOrReplaceTempView(temp_name);

        # Create statement to select all other columns in frame
        other_cols = [column for column in frame.columns if column != col];
        other_cols = "`, `".join(other_cols);
        other_cols = f"`{other_cols}`";

        # Create SQL expression to transform column
        query = f"""
        SELECT 
            CASE
                {query}
            END as `{col}`,
            {other_cols}
        FROM {temp_name}
        """;

        # Execute query 
        frame = self.__spark.sql(query);

        return frame;

    """
    Transpose frame
    
    @param frame: dataframe, the dataframe to transpose

    @return frame: dataframe, the transposed dataframe
    """
    def _transpose_frame(self,
        frame = None
    ):
        if frame is None:
            frame = self.__data;
        
        # Collect dataframe
        data = frame.collect(); 
        cols = frame.columns;
        
        # Get transposed data
        transposed_data = [];
        for col in cols:
            new_row = [col] + [row[col] for row in data];
            transposed_data.append(new_row);

        # Create new column names
        new_header = ["Attribute"] + [row[0] for row in data];

        return self.__spark.createDataFrame(transposed_data, new_header);
    
    """
    Collect a frame and return a dictionary representation

    @param frame: dataframe, the dataframe to collect

    @return frame_dict: dict, the dictionary representation of the dataframe
    """
    def _collect_frame_to_dict(self, frame,
        calculate_mean = True,
        calculate_cis = True,
        invert = False
    ):
        # Assume first column as index
        data_category = frame.columns[0];

        # Collect frame and turn into one dictionary
        datas = [row.asDict() for row in frame.collect()];
        col_labels = frame.columns[1:];
        index_labels = [row[data_category] for row in datas];

        if not invert:
            datas = {
                # value of first column : value of all other columns
                row[data_category]: {
                    "data" : list(row.values())[1:]
                } for row in datas
            };

        else: 
            datas = {
                # Value of first row (list of columns): value of all other rows
                col: {
                    "data": [data[col] for data in datas]
                } for col in col_labels
            };       

        # Calculate means and confidence intervals
        if calculate_mean:
            for key in datas:
                data = datas[key]["data"];
                mean = sum(data) / len(data); 
                
                datas[key]["mean"] = mean;

                if calculate_cis:
                    datas[key]["lower_ci"], datas[key]["upper_ci"] = self._calculate_confidence_intervals(data, mean);
        
        if invert:
            index_labels, col_labels = col_labels, index_labels;

        datas["metadata"] = {
            "index_labels": index_labels,
            "col_labels": col_labels
        };

        # Get means and CIs for all columns
        if calculate_mean:
            col_means = [
                np.mean(
                    # Get value at position 'index' for each row
                    [datas[key]["data"][index] for key in index_labels]
                # Index for each column
                ) for index in range(len(col_labels))
            ];

            datas["metadata"]["col_means"] = col_means;

            if calculate_cis:
                # Get CIs as list of tuples (lower, upper)
                col_cis = [
                    self._calculate_confidence_intervals(
                        [datas[key]["data"][index] for key in index_labels],
                        mean
                    ) for index, mean in enumerate(col_means)
                ];
                
                # Get lists of lower and upper CIs
                lower_cis, upper_cis = zip(*col_cis);
                lower_cis = list(lower_cis);
                upper_cis = list(upper_cis);

                datas["metadata"]["col_lower_cis"], datas["metadata"]["col_upper_cis"] = lower_cis, upper_cis;

        return datas;

    """
    Calculate the confidence intervals around a given mean

    @param data: list of data to calculate confidence interval around
    @param alpha: significance level

    @return: tuple of lower and upper bounds of confidence interval
    """
    def _calculate_confidence_intervals(self, data,
        mean = None,
        alpha = 0.05,
        n_places = 5,
    ):
        # Compute mean if needed 
        if not mean:
            mean = np.mean(data);

        # Calculate standard error 
        std_dev = np.std(data);
        n = len(data);
        standard_error = std_dev / np.sqrt(n);

        # Get T value
        degrees_freedom = n - 1;
        t_value = t.ppf(1 - alpha / 2, degrees_freedom);
        
        
        # Calculate confidence intervals
        lower_bound = mean - t_value * standard_error;
        upper_bound = mean + t_value * standard_error;

        # Round confidence intervals to n decimal places to convert from numpy float to python float
        lower_bound = round(lower_bound, n_places);
        upper_bound = round(upper_bound, n_places);

        return lower_bound, upper_bound;
    
    """ 
    Get inferred case of values in each string column in the data based on a sample

    @param sample_size: int, the number of values to sample
    @param minor_words: list of str, the minor words to exclude from proper case

    @return case_mapping: dict, the mapping of each string column to its inferred case
    """
    def __get_case_mapping(self,
        sample_size = 50,
        minor_words = ["and", "or", "the", "a", "an", "in", "on", "at", "to", "of", "for", "by", "with", "from", "upon"]
    ):
        self.__minor_words = minor_words;
        self.__case_mapping = {};
        
        # Get all columns with a string data type
        string_cols = [col for col in self.__data.columns if str(self.__data.schema[col].dataType) == "StringType()"];
        
        # Get sample to infer cases from
        subset = self._get_first_values(n = sample_size);
        
        for string_col in string_cols:
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

            elif case == "unknown":
                raise ValueError(f"Case of '{string_col}' could not be inferred!");

            else:
                self.__case_mapping[string_col] = case;
        
    """
    Infer case of a string

    @param string: str, the string to infer the case of

    @return case: str, the inferred case of the string
    """
    def __infer_case(self, string):
        # Trim string
        string = string.strip();

        # Not composed of words 
        if string.count("/") == 2:
            return "date";
        if string.isnumeric():
            return "numeric";
        if not all([char.isalnum() or char == " " or char == "-" for char in string]):
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
        
        # If case cannot be inferred, return "unknown"
        return "unknown";
         
    """
    Helper method to check if string is title

    @param string: str, the string to check

    @return is_title: bool, whether the string is title case
    """
    def __is_title(self, string):
        return string[0].isupper();

    """
    Helper method to convert string to title

    @param string: str, the string to convert

    @return title: str, the string converted to title case
    """
    def __to_title(self, string):
        return string[0].upper() + string[1:];

    """
    Convert a string to a specified case

    @param string: str, the string to convert
    @param case: str, the case to convert the string to

    @return converted_string: str, the string converted to the specified case
    """
    def __convert_case(self,
        string = None,
        case = None,
    ):

        if case == "lower":
            return string.lower();
        elif case == "upper":
            return string.upper();

        elif case == "title":
            return self.__to_title(string);
        
        if case == "sentence" or case == "proper":
            words = string.split(" "); 

            if case == "sentence":
                words = [self.__to_title(word) if index == 0 else word.lower() for index, word in enumerate(words)];
            
            else:
                words = [self.__to_title(word) if word.lower() not in self.__minor_words else word.lower() for word in words];
            
            return " ".join(words);

        else:
            return string;
    
    """
    Convert all non-word string columns to non-string columns

    @param string_col: str, the column to convert
    @param case: str, the case to convert the column to
    @param reference_date: str, the reference date to base date conversions on
    @param date_format: str, the assumed format of column of dates
    """
    def __convert_str_col(self,
        string_col = None,
        case = None,
        reference_date = "2019-01-09",
        date_format = "dd/MM/yyyy"
    ):
        # Date: cast to int, number of days until reference day
        if case == "date":
            self.__data = self.__data.withColumn(
                string_col, 
                F.datediff(
                    F.lit(reference_date), 
                    F.to_date(F.col(string_col), date_format)
                )
            );
        
        # Numeric: cast to int
        elif case == "numeric":
            self.__data = self.__data.withColumn(string_col, F.col(string_col).cast("int"));
        
        # Symbol: convert all symbols to 0, then cast to int
        elif case == "symbol":
            self.__data = self.__data.withColumn(
                string_col, 
                F.regexp_replace(
                    F.col(string_col),
                    "[^0-9]", "0"
                ).cast("int")
            );

    """
    Round all doubles to n decimal places
    
    @param frame: dataframe, the dataframe to round
    @param n: int, the number of decimal places to round to
    """
    def __round_numeric_cols(self,
        frame = None,
        n = 5,
    ):
        if frame is None:
            frame = self.__data;

        for col in frame.columns:
            if str(frame.schema[col].dataType) == "DoubleType()":
                frame = frame.withColumn(
                    col, 
                    F.round(F.col(col), n)
                );

    """
    Produce dataframe of aggregates of data, by all values of a single column and row label

    @param filter_cols: list of str, the columns to filter by
    @param filter_passes: list of str, the values in filter_cols to filter by
    @param data: str, the data to aggregate
    @param row: str, the row label to aggregate by
    @param col: str, the column label to aggregate by
    @param count: bool, whether to count the data instead of summing it
    @param normalise: bool, whether to normalise the count

    @return frame: dataframe, the aggregated frame
    """
    def _get_agg_frame(self, 
        filter_cols = [],
        filter_passes = [],
        data = None, 
        row = None,
        col = None,
        count = False,
        normalise = False,
        mean = False,
    ):
        # Get all columns required to complete query
        requested_cols = [x for x in [row, col, data] if x is not None];

        frame = self._get_frame(
            requested_cols = requested_cols,
            filter_cols = filter_cols, 
            filter_passes = filter_passes,
        ); 
        
        # Transpose and aggregate to get requested frame 
        frame = self.__get_grouped_frame(
            frame = frame,
            row = [row],
            col = col,
            data = data,
            count = count,
            mean = mean,
            normalise = normalise,
        );

        return frame;
    
    """
    Get a subframe with given columns

    @param frame: dataframe, the dataframe to get the subset from
    @param requested_cols: list of str, the columns to select
    @param filter_cols: list of str, the columns to filter by
    @param filter_passes: list of str, the values in filter_cols to filter by
    @param or_and: str, the operator to use when filtering by selected rows

    @return frame: dataframe, the subset of the dataframe
    """
    def _get_frame(self,
        frame = None,
        requested_cols = None,
        filter_cols = [],
        filter_passes = [],
        or_and = "and",
    ):
        if frame is None:
            frame = self.__data;
        
        if requested_cols is None:
            requested_cols = frame.columns;
        
        # Create filter query
        filter_query = self.__create_filter_query(
            frame = frame,
            filter_cols = filter_cols, 
            filter_passes = filter_passes, 
            or_and = or_and,
        );
        
        # Select requested columns and filter by selected rows
        frame = frame.select(requested_cols).filter(filter_query);
        
        # Remove rows with missing data
        frame = frame.dropna();

        return frame;
    
    """
    Generate a filter query to select rows from a frame 

    @param frame: dataframe, the dataframe to get the subset from
    @param filter_cols: list of str, the columns to filter by
    @param filter_passes: list of str, the values in filter_cols to filter by
    @param or_and: str, the operator to use when filtering by selected rows

    @return query: str, the filter query
    """
    def __create_filter_query(self,
        frame = None,
        filter_cols = [],
        filter_passes = [],
        or_and = None,
    ):

        # Convert filter passes to correct case and ensure they exist
        filter_passes = [list(set(filter_pass)) for filter_pass in filter_passes];
        if len(filter_cols) != len(filter_passes):
            raise ValueError(f"Number of filter columns {filter_cols} len = {len(filter_cols)} and filter passes {filter_passes} len = {len(filter_passes)} must be equal!");

        # For every column and values to be filtered by
        for index, filter_col in enumerate(filter_cols):
            # Convert any integer arguments to string
            filter_passes[index] = [str(filter_pass) for filter_pass in filter_passes[index]];

            
            if filter_col in self.__case_mapping:
                case = self.__case_mapping[filter_col];

            for idx, filter_pass in enumerate(filter_passes[index]):
                fp = filter_pass;
                 
                # Convert case of filter_pass to match case of column
                if "case" in locals():
                    fp = self.__convert_case(fp, case); 

                # Rename edge case values
                if fp in self.__manual_case_renames:
                    fp = self.__manual_case_renames[fp];
                
                filter_passes[index][idx] = fp;

            selected_filter_passes = filter_passes[index];
            
            # Check if any values in filter_passes are missing
            missing_filter_passes = [
                filter_pass for filter_pass in selected_filter_passes 
                if not self.__value_exists(
                    frame = frame, 
                    col = filter_col, 
                    value = filter_pass
                )
            ];

            # Raise error if any values are missing
            if len(missing_filter_passes) > 0:
                missing_filter_passes = "', '".join(missing_filter_passes);
                raise ValueError(f"Rows '{missing_filter_passes}' not found in {filter_col}!");
        
        # For each filter, combine all pass values into a single string 
        filter_passes = ["', '".join(filter_pass) for filter_pass in filter_passes];

        # Create SQL filter query for each filter
        queries = [
            f"{filter_col} in ('{filter_passes[index]}')" for index, filter_col in enumerate(filter_cols)
        ];
    
        # Join all queries together
        query = f" {or_and} ".join(queries);
        
        return query;

    """
    Setter for manual case renames
    """
    def _set_manual_case_renames(self, manual_case_renames):
        self.__manual_case_renames = manual_case_renames;

    """
    Check if a value exists in a column

    @param frame: dataframe, the dataframe to check
    @param col: str, the column to check
    @param value: str, the value to check

    @return exists: bool, whether the value exists in the column
    """
    def __value_exists(self,
        frame = None,
        col = None,
        value = None
    ):
        # Since we are only checking if the value exists, we can limit the count to 1
        return frame.filter(F.col(col) == value).limit(1).count() > 0;

    """
    Group by, pivot and aggregate over a frame

    @param frame: dataframe, the dataframe to get the subset from
    @param col: str, the column to use as the column of each frame
    @param row: str, the row label to aggregate by
    @param data: str, the data to aggregate
    @param count: bool, whether to count the data instead of summing it
    @param normalise: bool, whether to normalise the count
    @param n_places: int, the number of decimal places to round to
    @param mean: bool, whether to calculate the mean of the data

    @return frame: dataframe, the aggregated frame
    """
    def __get_grouped_frame(self,
        frame = None,
        col = None,
        row = None,
        data = None,
        count = None,
        normalise = None,
        n_places = 5,
        mean = None,
    ):
        frame = frame.groupBy(row);

        # If count is requested, return count of data
        if count:
            frame = frame.pivot(col).count();

            if not normalise:
                return frame;
            
            # Create totals column with total counts of each row
            agg_cols = [col for col in frame.columns if col not in row];
            total_expr = sum(F.col(agg_col) for agg_col in agg_cols);
            frame = frame.withColumn("total", total_expr);
            
            # Normalise data by dividing by total
            for agg_col in agg_cols:
                frame = frame.withColumn(
                    agg_col, 
                    F.col(agg_col) * 100 / F.col("total")
                );

            # Drop total column
            frame = frame.drop("total");
            
            return frame;
    
        # Pivot to get proper rows, and aggregate
        if data is not None:
            frame = frame.pivot(col);
            if mean:
                return frame.agg(F.mean(data));
            else:
                return frame.sum(data);

        # If no data provided, assume "col" is data and no pivot requested
        else:
            if mean:
                return frame.agg(F.mean(col));
            else:
                return frame.sum(col);
            
    """
    Get a frame with aggregates of multiple columns 
    @param frame: dataframe, the dataframe to get the subset from
    @param rows: list of str, the row labels to check
    @param selected_rows: list of str, the selected rows to filter by
    @param datas_category: str, the category of all datas to aggregate
    @param datas: list of str, the datas to aggregate
    @param col: str, the column to use as the column of each frame
    """
    def _get_multi_col_agg_frame(self,
        frame = None,
        filter_cols = [], 
        filter_passes = [],
        cols_category = None,
        cols = [],
        row = None
    ):
        # Get frame from data if not provided
        if frame is None:
            requested_cols = cols + [row];
            frame = self._get_frame(
                requested_cols = requested_cols,
                filter_cols = filter_cols,
                filter_passes = filter_passes
            );
        
        # Create a stack expression 
        n_datas = len(cols);
        stack_expr = f"""
            stack(
                {n_datas},
                {", ".join([f"'{col}', {col}" for col in cols])}
            ) as ({cols_category}, count)
        """;

        # Unpivot frame by stacking data columns
        frame = frame.selectExpr(row, stack_expr);
        
        # Group by and pivot frame
        frame = self.__get_grouped_frame(
            frame = frame,
            row = cols_category,
            col = row, 
            data = "count"
        );
        
        # Sort data by last column
        last_col = frame.columns[-1];
        frame = frame.orderBy(frame[last_col].asc());
        
        return frame;
    
    """
    Helper function to find column of first instance of given value

    @param needle: str, the value to search for
    @param frame: dataframe, the frame to search in

    @return col: str, the column of the first instance of the value
    """
    def _get_first_instance_col(self, needle,
        frame = None,
        no_convert = False,
    ):
        if frame is None:
            frame = self.__data;
        
        # Manually rename needle if required
        if needle in self.__manual_case_renames:
            needle = self.__manual_case_renames[needle];
            no_convert = True;

        for col in self._get_cols():
            transformed_needle = needle;
            # Convert case of needle to match case of column
            if no_convert is False and col in self.__case_mapping:
                case = self.__case_mapping[col];
                transformed_needle = self.__convert_case(needle, case);
            
            if self.__value_exists(
                frame = frame,
                col = col,
                value = transformed_needle
            ):
                return col;

        raise ValueError(f"Value '{needle}' not found in data!");

    """
    Get multiple aggregated frames containing one type of data
    @param datas: list of str, the datas to aggregate
    @param rows: list of str, rows of frames 
    @param cols: list of str, cols of frames
    @param titles: list of str, titles of frames
    @param default_col: str, default column
    """
    def _get_batch_agg_frames(self,
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
            frame = self._get_agg_frame(
                data = data,
                row = rows[index],
                col = cols[index]
            );

            # Assign frame to frames dictionary
            frames[titles[index]] = frame;

        return frames;

    """
    Produces multiple aggregated frames containing multiple types of data
    @param title_col: str, the column to use as the title
    @param titles: list of str, the titles to use
    @param datas_category: str, overall category of all datas to aggregate
    @param datas: list of str, the datas to aggregate
    @param col: str, the column to use as the column of each frame
    @param col_prefix: str, the prefix to remove from values of the column
    """
    def _get_batch_multi_agg_frames(self,
        title_col = None,
        titles = None,
        datas_category = None,
        datas = None,
        col = "time_period",
        col_prefix = None
    ):
        frames = {};
        
        # Get initial frame containing all data
        initial_frame = self._get_frame(
            requested_cols = [title_col, col] + datas
        );
        
        for title in titles:
            # Get frame with only data for title
            frame = self._get_frame(
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
                F.regexp_replace(datas_category, col_prefix, "")
            ); 

            # Assign frame to frames dictionary
            frames[title] = frame;

        return frames;

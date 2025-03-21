from spark_data import SparkData;

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, VectorSlicer, Interaction;
from pyspark.ml import Pipeline;

from pyspark.sql.functions import log;
import pyspark.sql.functions as F;

import numpy as np;
from pyspark.ml.stat import Correlation;

from pyspark.ml.regression import GeneralizedLinearRegression;

import math;

"""
Class representing the absences data specifically
"""
class Absences(SparkData):
    """
    Constructor: loads data and gets initial distinct values

    @param absences_loc: str, the location of the absences data
    """
    def __init__(self,
        absences_loc = "data/Absence_3term201819_nat_reg_la_sch.csv",
        total_school_type = "Total",
    ):
        # Load the data
        super().__init__(absences_loc);
        
        # Get distinct values required for constructing default filters
        self.__distinct_regions = self.__get_distinct_values("region_name");
        self.__all_distinct_school_types = self.__get_distinct_values("school_type");

        if total_school_type in self.__all_distinct_school_types:
            self.__totaless_distinct_school_types = [school_type for school_type in self.__all_distinct_school_types if school_type != total_school_type];

        self.__absence_reasons = self.__get_absence_reasons();

        # Turn yyyy/yy to yyyy
        self.__clean_years = self.__clean_time_period();

        self.__manual_case_renames = {
            "Isles of Scilly": "Isles Of Scilly",
        };

    """
    Create dictionary mapping yyyyyy to yyyy/yy
    """
    def __clean_time_period(self):
        # Get distinct years as strings
        years = self.__get_distinct_values("time_period");
        years = [str(year) for year in years];

        # Create dictionary mapping inserting a '/' after the 4th character 
        clean_years = {
            year: f"{year[:4]}/{year[4:]}" 
        for year in years};

        return clean_years;
    
    """
    Helper function to get distinct rows and convert into a list of strings

    @param row: str, the column to get distinct values of 

    @return list of str, the distinct values of the column
    """
    def __get_distinct_values(self, row):
        # Get list of rows of regions, and convert to list of strings
        distincts = self._get_distinct_values(row);
        distincts = [distinct.asDict() for distinct in distincts]; 
        return  [distinct[row] for distinct in distincts];

    """
    Helper method to add default filter settings to arguments

    @param filter_cols: list of str, the columns to filter by
    @param default_filter_cols: list of str, the default columns to add. Default geographic_level to avoid totals appearing in aggregation
    @param filter_passes: list of str, the values to filter by
    @param default_filter_passes: list of str, the default values to add. Default national to access as few records as possible

    @return tuple of list of str, list of str, the combined filter columns and passes
    """
    def __add_default_filters(self, 
        filter_cols = None,
        default_filter_cols = ["geographic_level", "school_type"],
        filter_passes = None,
        default_filter_passes = [["National"], ["Total"]],
    ):         

        filter_cols = filter_cols + default_filter_cols;
        filter_passes = filter_passes + default_filter_passes;

        return filter_cols, filter_passes;
    
    """
    Get dictionary of samples of keys to use as default values

    @param keys: dict of str, the keys to get default values for and the number of values to get

    @return dict of str, the default values for the keys
    """
    def get_default_values(self,
        keys = {
            "school_type" : 3,
            "time_period": 1,
            "la_name" : 10,
            "region_name" : 3
        },
    ):
        # Create a sample with the size of the smallest value required to satisfy all keys
        n = max(keys.values());
        first_values = self._get_first_values(n = n);
        
        # Get values of each key in each row
        rows = [{key: first_value[key] for key in keys} for first_value in first_values];

        # Get inverted dictionary
        dictionary = {key: [row[key] for row in rows] for key in keys};

        # Limit the number of values to the number requested
        dictionary = {key: dictionary[key][:keys[key]] for key in keys};

        # Set single level lists to single values
        for key in keys:
            if len(dictionary[key]) == 1:
                dictionary[key] = dictionary[key][0];

        return dictionary;

    """
    Get number of pupil enrolments for a requested local authority for each year

    @param geographic_levels: list of str, the levels of geography to filter by
    @param filter_cols: list of str, the columns to filter by
    @param local_authorities: list of str, the local authorities to filter by
    @param row: str, the column to use as rows
    @param col: str, the column to use as columns
    @param data: str, the column to use as data

    @return DataFrame, the number of pupil enrolments for the requested local authorities
    """
    def get_enrolment_by_la_over_time(self,
        geographic_levels = ["Local authority"],
        filter_cols = ["la_name"],
        local_authorities = [],
        row = "la_name",
        col = "time_period",
        data = "enrolments",
        col_renames = {
            "la_name": "Local authority",
        }
    ):
        local_authorities = self.__get_distinct_values("la_name");

        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [local_authorities],
            default_filter_passes = [geographic_levels, ["Total"]],
        );    

        # Get enrolment data for the requested local authorities
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            data = data,
            row = row,
            col = col,
            manual_case_renames = self.__manual_case_renames
        );
        
        # Add time period mapping to rename columns
        col_renames = self.__add_time_period_renames(col_renames); 

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );
        
        return frame;
    
    """
    Add time period mapping to rename columns

    @param col_renames: dict of str, the column names to rename

    @return dict of str, the column names to rename
    """
    def __add_time_period_renames(self, col_renames):
        [col_renames.update(
            {col: self.__clean_years[col]}
        ) for col in self.__clean_years]; 

        return col_renames;
    
    """
    Get authorised absence data for requested school types and years

    @param school_types: list of str, the school types to filter by
    @param years: list of str, the years to filter by
    @param row: str, the column to use as rows
    @param col: str, the column to use as columns
    @param sess_prefix: str, the prefix to remove from the column names

    @return DataFrame, the authorised absence data for the requested school types
    """
    def get_auth_by_school_type(self,
        filter_cols = ["time_period"],
        school_types = [],
        years = [],
        row = "school_type",
        col = "sess_authorised",
        col_renames = { 
            "school_type": "School type",
            "sum(sess_authorised)": "Authorised absences"
        }
    ):
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [years],
            default_filter_passes = [["National"], school_types]
        ); 

        # Get authorised absence data for the requested school types and year
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            row = row,
            col = col
        );
        
        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );

        return frame;

    """
    Get authorised absence data by absence reasons for requested school types and years

    @param school_types: list of str, the school types to filter by
    @param years: list of str, the years to filter by
    @param cols_category: str, the category of columns to use
    @param absence_reasons: list of str, list of reasons for absence
    @param row: str, the column to use as rows
    @param authorised_prefix: str, the prefix to remove from the column names

    @return DataFrame, the authorised absence data by absence reasons for the requested school types
    """
    def get_auth_by_school_type_detailed(self,
        filter_cols = ["time_period"],
        school_types = [],
        years = [],
        cols_category = "absence_reason",
        absence_reasons = [],
        row = "school_type",
        authorised_prefix = "sess_auth_",
        col_renames = {
            "absence_reason": "Reason for authorised absence"
        }
    ):
            
        # Get every absence reason 
        if not absence_reasons:
            absence_reasons = self.__absence_reasons; 

        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [years],
            default_filter_passes = [["National"], school_types]
        );

        # Get one frame with absence reasons as rows and school types as columns 
        frame = self._get_multi_col_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            cols_category = cols_category,
            cols = absence_reasons,
            row = row
        );
        
        clean_absence_reason_query = self.__clean_col(
            prefix = authorised_prefix,
            col_category = cols_category,
            special_cases = {
                "sess_auth_ext_holiday": "Extended holiday",
                "sess_auth_totalreasons": "Total"
            },
        );

        frame = self._transform_col(
            frame = frame,
            col = cols_category,
            query = clean_absence_reason_query
        );

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );

        return frame;
    
    # TODO automatically pass this SQL to the _transform_col method
    """
    Create an SQL query to clean a column with custom values for special cases and standard cleaning for the rest

    @param prefix: str, the prefix to remove from the column names
    @param col_category: str, the category of columns to clean
    @param special_cases: dict of str, the special cases to clean
    
    @return str, the query to clean the column
    """
    def __clean_col(self,
        prefix = None,
        col_category = None,
        special_cases = {}
    ):
        special_cases = [f"WHEN `{col_category}` = '{key}' THEN '{value}'" for key, value in special_cases.items()];
        special_cases = "\n".join(special_cases);

        return f"""
            {special_cases} 
            ELSE
                initcap(
                    regexp_replace(
                        regexp_replace(`{col_category}`, '^{prefix}', ''),
                    '_', ' ')
                )
        """

    """
    Get all column names which are reasons for absence

    @return list of str, the names of the columns which are reasons for absence
    """
    def __get_absence_reasons(self):
        return [col for col in self._get_cols() if self.__is_reason_for_absence(col)];

    """
    Helper method to check if column is a reason for absence

    @param col: str, the column to check
    @param prefix: str, the prefix to check for

    @return bool, whether the column is a reason for absence
    """
    def __is_reason_for_absence(self, col,
        prefix = "sess_auth_"
    ):
        return col.startswith(prefix);
    
    """
    Get unauthorised absences in a requested year by region name or local authority name
    """
    def get_unauth_by_la_region(self,
        geographic_levels = ["Local authority"],
        region_or_la = [],
        years = [],
        col = "sess_unauthorised",
        sess_prefix = "sess_",
        col_renames = {
            "sum(sess_unauthorised)": "Total unauthorised absences"
        },
    ):
        # Determine whether inputs are regions or local authorities
        cols_of_input = [self._get_first_instance_col(name) for name in region_or_la];
        
        # Include either region name and/or local authority name in filter columns
        filter_cols = [];
        if "region_name" in cols_of_input:
            filter_cols.append("region_name");
            col_renames["region_name"] = "Region";

        if "la_name" in cols_of_input:
            filter_cols.append("la_name");
            col_renames["la_name"] = "Local authority";

        if len(filter_cols) == 2:
            raise ValueError("Both region and local authority names were provided. Please provide one or the other.");
        
        row = filter_cols[0];
        
        # Add time period to filter columns
        filter_cols.append("time_period");

        # Add default filters as usual 
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [region_or_la, years],
            default_filter_passes = [geographic_levels, ["Total"]]
        );

        # Get unauthorised absence data for the requested regions or local authorities
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            col = col,
            row = row,
            manual_case_renames = self.__manual_case_renames
        );

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );

        return frame;
    
    """ 
    Produce data for comparing local authorities in a given year

    @param local_authorities: list of str, the local authorities to compare
    @param years: list of str, the years to compare
    @param cols: list of str, the datas to compare local authorities by

    @return DataFrame, the data for the requested local authorities
    """
    def compare_la_in_year(self,
        geographic_levels = ["Local authority"],
        filter_cols = ["la_name", "time_period"],
        local_authorities = [],
        years = [],
        cols_category = "important_stats",
        cols = [], 
        row = "la_name",
        authorised_prefix = "sess_",
        col_renames = {
            "important_stats": "Comparison statistics" 
        }
    ):
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [local_authorities, years],
            default_filter_passes = [geographic_levels, ["Total"]]
        );
        
        # Get one frame with important stats as rows and local authorities as columns 
        frame = self._get_multi_col_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            cols_category = cols_category,
            cols = cols,
            row = row,
            manual_case_renames = self.__manual_case_renames
        );

        clean_important_stats_query = self.__clean_col(
            prefix = authorised_prefix,
            col_category = cols_category,
            special_cases = {
                "enrolments_pa_10_exact_percent": "Persistent absentees percent",
                "sess_overall_percent_pa_10_exact": "Persistent absentee overall absence percent"
            },
        );

        frame = self._transform_col(
            frame = frame,
            col = cols_category,
            query = clean_important_stats_query,
        );

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );

        datas = self._collect_frame_to_dict(frame);
        
        return frame, datas;

    """
    Produce data comparing all regions over all time periods
    """
    def compare_region_attendance_over_time(self,
        geographic_levels = ["Regional"],
        filter_cols = ["region_name"],
        regions = [],
        row = "region_name",
        col = "time_period",
        data = None,
        col_renames = {
            "region_name": "Region"
        },
    ):
        # Get all regions if none are provided
        if len(regions) == 0:
            regions = self.__distinct_regions;

        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [regions],
            default_filter_passes = [geographic_levels, ["Total"]]
        );
        
        # Get enrolment data for the requested local authorities
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            data = data,
            row = row,
            col = col
        );

        col_renames = self.__add_time_period_renames(col_renames); 

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );
        
        datas = self._collect_frame_to_dict(frame);

        return frame, datas;
    
    """ 
    Get data on absences by school type
    """
    def get_school_type_absences(self,
        school_types = [],
        filter_cols = [],
        filter_passes = [],
        row = "school_type",
        col = "sess_overall_percent",
        col_renames = {
            "avg(sess_overall_percent)": "Average overall absence percent per year",
            "school_type": "School type"
        }
    ):
        if not school_types:
            school_types = self.__all_distinct_school_types;

        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = filter_passes,
            default_filter_passes = [["National"], school_types]
        );
        
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            row = row,
            col = col,
            mean = True
        );

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );
        
        datas = self._collect_frame_to_dict(frame);

        return frame, datas;

    def get_absences_region(self,
        geographic_levels = ["Regional"],
        filter_cols = [],
        filter_passes = [],
        row = "region_name",
        col = "sess_overall_percent",
        col_renames = {
            "avg(sess_overall_percent)": "Average overall absence percentage per year",
            "region_name": "Region"
        }
    ):
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = filter_passes,
            default_filter_passes = [geographic_levels, ["Total"]]
        );
        
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            row = row,
            col = col,
            mean = True
        );

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );
        
        datas = self._collect_frame_to_dict(frame);

        return frame, datas;
    
    def get_region_school_type(self,
        geographic_levels = ["School"],
        school_types = [],
        filter_cols = [],
        filter_passes = [],
        row = "region_name",
        col = "school_type",
        col_renames = {
            "school_type": "School type",
            "region_name": "Region",
        }
    ):
        if school_types == []:
            school_types = self.__all_distinct_school_types;

        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = filter_passes,
            default_filter_passes = [geographic_levels, school_types]
        );
        
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            row = row,
            col = col,
            count = True,
            normalise = True,
        );

        frame = self._rename_cols(
            frame = frame,
            col_renames = col_renames
        );
        
        datas = self._collect_frame_to_dict(frame,
            invert = True
        );

        return frame, datas;
    
    """
    Return a fitted model for absences
    """
    def model_absences(self,
        framework = None,
        frame = None,
    ):
        if framework is None:
            framework = self.__get_model_framework();

        if frame is None:
            frame = self.__get_model_data();

        return framework.fit(frame);

    """
    Produce the dataset required to model absences
    """
    def get_model_data(self,
        geographic_levels = ["School"],
        filter_cols = [],
        filter_passes = [],
        response = "sess_overall",
        offset = "sess_possible",
        use_offset = True,
        factor_covariates = ["time_period", "region_name", "school_type"],
        numeric_covariates = ["enrolments", "sess_unauthorised_percent", "sess_overall_percent_pa_10_exact", "sess_unauthorised_percent_pa_10_exact", "enrolments_pa_10_exact_percent"],
        interaction_factor_terms = [("region_name", "school_type")],
        print_high_collinearity = True,
    ):
        # Get school-level data
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = filter_passes,
            default_filter_passes = [geographic_levels, self.__totaless_distinct_school_types]
        );

        requested_cols = [response] + factor_covariates + numeric_covariates + [offset];
        
        frame = self._get_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            requested_cols = requested_cols
        );
                
        # Apply pipeline to frame
        pipeline = self.__create_absence_modelling_pipeline(
            factor_covariates = factor_covariates,
            numeric_covariates = numeric_covariates,
            interaction_factor_terms = interaction_factor_terms
        );
        model_frame = pipeline.fit(frame).transform(frame);
        
        if print_high_collinearity:
            self.__print_high_collinearity(model_frame);
        
        # Create column of log of offset
        if use_offset:
            model_frame = model_frame.withColumn(f"log_{offset}", log(offset));

        return model_frame;

    """
    Return a framework for modelling absences
    """
    def __get_model_framework(self,
        offset = "sess_possible",
        response = "sess_overall",
    ):
        # Fit Poisson GLM with an offset
        return GeneralizedLinearRegression(
            family = "poisson",
            link = "log",
            featuresCol = "features",
            labelCol = response,
            offsetCol = f"log_{offset}",
        );
                   
    """
    Create a pipeline for transforming data in preparation for modelling absences
    """
    def __create_absence_modelling_pipeline(self,
        factor_covariates = [],
        numeric_covariates = [],
        interaction_factor_terms = [()],
    ):
        
        # Process factor covariates via indexing and creating dummy variables
        indexers = [StringIndexer(
            inputCol = covariate, 
            outputCol = f"{covariate}_index", 
            handleInvalid = "skip",
            stringOrderType = "alphabetDesc"
        ) for covariate in factor_covariates];
        encoders = [OneHotEncoder(
                inputCol = f"{covariate}_index", 
                outputCol = f"{covariate}_vec", 
                dropLast = True
        ) for covariate in factor_covariates];
        
        # Add interaction terms
        interaction_stages = [];
        for terms in interaction_factor_terms:
            col1, col2 = terms;
            interaction_stages.append(
                Interaction(
                    inputCols = [f"{col1}_vec", f"{col2}_vec"],
                    outputCol = f"{col1}_{col2}_interaction"
                )
            );


        # Assemble all features
        assembler_inputs = [f"{covariate}_vec" for covariate in factor_covariates] + numeric_covariates;
        for terms in interaction_factor_terms:
            col1, col2 = terms;
            assembler_inputs.append(f"{col1}_{col2}_interaction");

        assembler = VectorAssembler(inputCols = assembler_inputs, outputCol = "features");

        # Create pipeline
        pipeline = Pipeline(stages = indexers + encoders + interaction_stages + [assembler]);
        
        return pipeline;

    """
    Print out any features which have high collinearity
    """
    def __print_high_collinearity(self, frame,
        threshold = 0.9,
        idx_sort = False,
    ):
        # Get correlation matrix
        correlation = Correlation.corr(frame, "features");
        corr_matrix = correlation.collect()[0][0];

        # Convert to np array and find rows and cols with high collinearity
        corr_matrix = np.array(corr_matrix.toArray());

        # Mask is if the correlation is above the threshold or is NaN
        mask = (np.abs(corr_matrix) > threshold) | np.isnan(corr_matrix);

        # Remove diagonal and apply mask
        np.fill_diagonal(mask, False);
        rows, cols = np.where(mask);
        
        # Get feature names
        feature_names = self.get_feature_names(frame, idx_sort = idx_sort);

        # Get indices and names of features with high collinearity
        for i, j in zip(rows, cols):
            print(f"Features '{feature_names[i]}' ({i}) and '{feature_names[j]}' ({j}) have high collinearity: {corr_matrix[i, j]}");

    """
    Compute confidence intervals for the model
    """
    def get_model_confidence_intervals(self, model, coefficients,
        z = 1.96,
    ):
        # Extract coefficients and SEs
        standard_errors = model.summary.coefficientStandardErrors;

        # Transform from log scale to original scale
        standard_errors = [standard_error * coefficient for standard_error, coefficient in zip(standard_errors, coefficients)];

        # Assume normal distribution of SEs (reasonable given extreme sample size and CLT), and calculate confidence intervals
        lower, upper = zip(*[(coefficient - z * standard_error, coefficient + z * standard_error) for coefficient, standard_error in zip(coefficients, standard_errors)]);

        return lower, upper;

    """
    Scale coefficients of model to original scale
    """
    def scale_coefficients(self, coefficients):
        return [math.exp(coefficient) for coefficient in coefficients];

    """
    Get feature names from the model frame
    """
    def get_feature_names(self, frame,
        idx_sort = None,
    ):
        features = frame.schema["features"].metadata["ml_attr"]["attrs"];
        features = features["binary"] + features["numeric"];

        # Sort features by index
        if idx_sort:
            features.sort(key = lambda feature: feature["idx"]);

        # Get feature names
        feature_names = [feature["name"] for feature in features];

        return feature_names; 

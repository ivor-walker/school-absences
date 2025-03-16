from spark_data import SparkData

"""
Class representing the absences data specifically
"""
class Absences(SparkData):
    """
    Constructor: loads data
    @param absences_loc: str, the location of the absences data
    """
    def __init__(self,
        absences_loc = "data/Absence_3term201819_nat_reg_la_sch.csv"
    ):
        super().__init__(absences_loc);

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
        default_filter_cols = ["geographic_level"],
        filter_passes = None,
        default_filter_passes = [["National"]],
    ):         

        filter_cols = filter_cols + default_filter_cols;
        filter_passes = filter_passes + default_filter_passes;

        return filter_cols, filter_passes;

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
    def get_by_la(self,
        geographic_levels = ["Local authority"],
        filter_cols = ["la_name"],
        local_authorities = [],
        row = "la_name",
        col = "time_period",
        data = "enrolments"
    ):
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [local_authorities],
            default_filter_passes = [geographic_levels]
        );    

        # Get enrolment data for the requested local authorities
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            data = data,
            row = row,
            col = col
        );
    
        return frame;

    """
    Get authorised absence data for requested school types and years

    @param school_types: list of str, the school types to filter by
    @param years: list of str, the years to filter by
    @param row: str, the column to use as rows
    @param col: str, the column to use as columns
    @param sess_prefix: str, the prefix to remove from the column names

    @return DataFrame, the authorised absence data for the requested school types
    """
    def get_by_school_type(self,
        filter_cols = ["school_type", "time_period"],
        school_types = [],
        years = [],
        row = "school_type",
        col = "sess_authorised",
        sess_prefix = "sess_"
    ):
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [school_types, years] 
        ); 

        # Get authorised absence data for the school types
        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            row = row,
            col = col
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
    def get_by_school_type_detailed(self,
        filter_cols = ["school_type", "time_period"],
        school_types = [],
        years = [],
        cols_category = "absence_reasons",
        absence_reasons = [],
        row = "school_type",
        authorised_prefix = "sess_auth_",
    ):
            
        # Get every absence reason 
        if not absence_reasons:
            absence_reasons = self.__get_absence_reasons();
       
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [school_types, years] 
        );

        # Get one frame with absence reasons as rows and school types as columns 
        frame = self._get_multi_col_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            cols_category = cols_category,
            cols = absence_reasons,
            row = row
        );
        
        return frame;

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
        col = "sess_unauth",
        sess_prefix = "sess_"
    ):
        # Determine whether inputs are regions or local authorities
        cols_of_input = [self._get_first_instance_col(name) for name in region_or_la];
        
        # Include either region name and/or local authority name in filter columns
        filter_cols = [];
        if "region_name" in cols_of_input:
            filter_cols.append("region_name");

        if "la_name" in cols_of_input:
            filter_cols.append("la_name");

        if len(filter_cols) == 2:
            raise ValueError("Both region and local authority names were provided. Please provide one or the other.");
        
        row = filter_cols[0];

        # Create filters as usual
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [region_or_la, years],
            default_filter_passes = [geographic_levels]
        );

        # Get unauthorised absence data for the requested regions or local authorities

        frame = self._get_agg_frame(
            filter_cols = filter_cols,
            filter_passes = filter_passes,
            col = col,
            row = row
        );

        return frame;

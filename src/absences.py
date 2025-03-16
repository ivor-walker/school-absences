from spark_csv import SparkCSV

"""
Class representing the absences data specifically
"""
class Absences(SparkCSV):
    def __init__(self,
        absences_loc = "data/Absence_3term201819_nat_reg_la_sch.csv"
    ):
        self.default_filter_cols = ["geographic_level"];
        self.default_filter_passes = ["National"];

        super().__init__(absences_loc);

    """
    Helper method to add default filter columns and pass values
    """
    def __add_default_filters(self, 
        filter_cols = None,
        default_filter_cols = None,
        filter_passes = None,
        default_filter_passes = None
    ):         
        if default_filter_cols is None: 
            default_filter_cols = self.default_filter_cols;

        if default_filter_passes is None:
            default_filter_passes = self.default_filter_passes;

        filter_cols = filter_cols + default_filter_cols;
        filter_passes = filter_passes + default_filter_passes;

        return filter_cols, filter_passes;

    """
    Get number of pupil enrolments for a requested local authority for each year
    """
    def get_by_la(self, local_authorities,
        geographic_level = ["Local authority"]
        filter_cols = ["la_name"],
        local_authorities = [],
        row = "la_name",
        col = "time_period"
        data = "enrolments",
    ):
        filter_cols, filter_passes = self.__add_default_filters(
            filter_cols = filter_cols, 
            filter_passes = [local_authorities]
            default_filter_passes = [geographic_level]
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
    """
    def get_by_school_type(self,
        filter_cols = ["school_type", "time_period"],
        school_types = [],
        years = [],
        row = "school_type",
        col = "sess_authorised"
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
            data = data, 
            row = row,
            col = col
        );

        # Remove the prefix from the column names
        frame = frame.rename(columns = lambda x: x.replace(sess_prefix, ""));
    
        return frame;

    """
    Get authorised absence data by absence reasons for requested school types and years
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
            cols_category = rows_category,
            cols = absence_reasons,
            row = row
        );
        
        frame = frame.rename(columns = lambda x: x.replace(authorised_prefix, ""));
        
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
    """
    def __is_reason_for_absence(self, col):
        return col.startswith("sess_auth_");


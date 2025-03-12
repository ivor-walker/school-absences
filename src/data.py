from pyspark.sql import SparkSession

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
        create_spark_session();

        load_data(csv_loc = absences_loc);

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
            .csv(self.csv_loc)
        );

    """
    Produce aggregates of data by all values of a single column and row label
    @param data: str, label of data values to use
    @param row: str, label of rows values to use
    @param col: str, label of column values to use
    @param filter: str, filter to apply to the data
    """
    def get_table(self, 
        data = None, 
        row = None, 
        col = None, 
        filter = None
    ):
        # Get data, row and col data as columns from original data 
        data = self.data.select(data, row, col);

        # Apply filter
        if filter:
            data = data.filter(filter);

        # Transpose data
        data = data.groupBy(row).pivot(col).sum(data);

        return data;
    
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
        data = self.data.select(data, *rows, col);

        # Apply filter
        if filter:
            data = data.filter(filter);

        # Transpose data
        data = data.groupBy(*rows).pivot(col).sum(data);

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

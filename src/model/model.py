from pyspark.ml.regression import GeneralizedLinearRegression;

import math;

"""
Statistical model result from absences.py
"""
class Model:
    """
    Constructor: use input data to fit model and calculate summary statistics
    """
    def __init__(self, frame, feature_names):
        self.__frame = frame;
        self.__feature_names = feature_names;

        self.__model = self.__model_absences(frame = frame);
        self.__scaled_coefficients = self.__scale_coefficients(self.__model.coefficients);
        self.__lower, self.__upper = self.__get_model_confidence_intervals(self.__model.summary.coefficientStandardErrors, self.__scaled_coefficients);
        
        self.__model_summary = self.__get_model_summary();
        self.__detailed_model_summary = self.__get_detailed_model_summary();

    """ 
    Return a fitted model for absences
    """
    def __model_absences(self,
        framework = None,
        frame = None,
    ):
        if framework is None:
            framework = self.__get_model_framework();

        if frame is None:
            frame = self.__get_model_data();

        return framework.fit(frame);

    """
    Return a framework for modelling absences
    """
    def __get_model_framework(self,
        offset = "sess_possible",
        response = "sess_overall",
    ):
        # Poisson GLM with an offset
        return GeneralizedLinearRegression(
            family = "poisson",
            link = "log",
            featuresCol = "features",
            labelCol = response,
            offsetCol = f"log_{offset}",
        );

    """
    Getter for model sumary
    """
    def __get_model_summary(self):
        summary_warning = """
        Warning: This model is overdispersed, so standard errors and p-values are artificially low and all variables falsely appear significant. Spark has no built-in way to handle overdispersion, so please focus on interpreting coefficient sizes and ignore confidence intervals for now. In the future I will conduct a power analysis to determine the sample size needed, then collect a sample of that size and use a more complete GLM library (or even import into R) that supports quasi-Poisson/negative binomial approaches to addressing overdispersion.
        """
        model_summary = summary_warning + "\n\n" + str(self.__model.summary);
        return model_summary;

    """
    Getter for model summary
    """
    def get_model_summary(self):
        return self.__model_summary;
    
    """
    Calculate detailed model summary
    """
    def __get_detailed_model_summary(self,
        decimal_places = 4,
    ):
        model_summary = self.__model_summary + "\n\nDetailed results:";
        
        for i in range(len(self.__feature_names)):
            # Add to model summary
            model_summary += f"\nfeature name: {self.__feature_names[i]}, coefficient: {round(self.__scaled_coefficients[i], decimal_places)}, lower: {round(self.__lower[i], decimal_places)}, upper: {round(self.__upper[i], decimal_places)}";

        return model_summary;
    
    """
    Getter for detailed model summary
    """
    def get_detailed_model_summary(self):
        return self.__detailed_model_summary;

        
    """
    Getter for feature names
    """
    def get_feature_names(self):
        return self.__feature_names;

    """ 
    Scale coefficients of model to original scale
    """
    def __scale_coefficients(self, coefficients):
        return [math.exp(coefficient) for coefficient in coefficients];

    """
    Getter for scaled coefficients
    """
    def get_scaled_coefficients(self):
        return self.__scaled_coefficients;

    """ 
    Compute confidence intervals for the model

    @param model: GeneralizedLinearRegressionModel, the model to get confidence intervals for
    @param standard_errors: list, the standard errors of the coefficients
    @param z: float, the z value for the confidence interval (default z yields 95% confidence interval)
    """
    def __get_model_confidence_intervals(self, standard_errors, coefficients,
        z = 1.96,
    ):
        # Transform from log scale to original scale
        standard_errors = [standard_error * coefficient for standard_error, coefficient in zip(standard_errors, coefficients)];

        # Assume normal distribution of SEs (reasonable given extreme sample size and CLT), and calculate confidence intervals
        lower, upper = zip(*[(coefficient - z * standard_error, coefficient + z * standard_error) for coefficient, standard_error in zip(coefficients, standard_errors)]);

        return lower, upper;

    """
    Getter for confidence intervals
    """
    def get_confidence_intervals(self):
        return self.__lower, self.__upper;


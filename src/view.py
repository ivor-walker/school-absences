import matplotlib.pyplot as plt;
import matplotlib.cm as cm;

import math;
import numpy as np;
from scipy.stats import t;

"""
Class for getting input and displaying output to the user
"""

class View:
    """
    Prompt the user for input
    @param prompt: str, the prompt to display to the user
    @param type: str, the type of input to get from the user
    """
    def prompt_user(self,
        prompt = None, 
        type = "str",
        split_char = ","
    ):
        if type == "list": 
            return self.__prompt_for_list(prompt, split_char);

        elif type == "int":
            return self.__prompt_for_int(prompt);

    """
    Ask for and recieve list input from the user
    """
    def __prompt_for_list(self, prompt, split_char):
        # Ask user to input entire list seperated by split char
        list_prompt = f"{prompt} (separated by {split_char}): ";
        user_input = input(list_prompt);
        
        try:
            # Remove whitespace in list
            user_input = user_input.split(split_char);
            user_input = [item.strip() for item in user_input];
            split_char.join(user_input);

            return user_input;

        except: 
            print("Invalid list.");
            return self.__prompt_for_list(prompt, split_char);
        
    """
    Ask for and recieve a int from the user
    """
    def __prompt_for_int(self, prompt):
        # Ask user to input int
        int_prompt = f"{prompt}: ";
        user_input = input(int_prompt);

        # Check user input is a int
        try:
            integer = int(user_input);
            return integer;

        except:
            print("Invalid integer.");
            return self.__prompt_for_int(prompt);

    """
    Display a Spark dataframe to the user
    @param frame: frameframe to display
    """
    def display_frame(self, frame):
        # Show all columns in entirety
        frame.show(frame.count(), False);
       
    """
    Display multiple bar charts

    @param frame: Spark dataframe to display
    @param n_cols: number of columns in grid of bar charts
    @param colourmap: colour scheme to use
    @param mean_line_colour: colour of mean line (None to disable)
    @param confidence_intervals_colour: colour of confidence intervals (None to disable)
    @param label_rotation: rotation of x-axis labels
    @param figsize: size of figure
    @param bottom: bottom margin of figure
    @param hspace: vertical spacing between subplots
    """
    def display_bar_charts(self, frame,
        n_cols = 2,
        colourmap = "viridis",
        mean_line_colour = "red", 
        confidence_intervals_colour = "red",
        label_rotation = 10,
        figsize = (15, 20),
        top = 0.93,
        bottom = 0.1, 
        hspace = 0.175,
        wspace = 0.08,
    ):
        data_category = frame.columns[0];
        
        # Collect frame and turn into one dictionary
        datas = [row.asDict() for row in frame.collect()];
        datas = {
                # value of first column : value of all other columns
                row[data_category]: list(row.values())[1:]
            for row in datas
        };

        # Get column and row labels
        col_labels = frame.columns[1:];
        row_labels = list(datas.keys());
        len_row_labels = len(row_labels);

        # Set up subplot grid, figure and title
        n_rows = math.ceil(
            len_row_labels / n_cols
        );
        fig, axs = plt.subplots(n_rows, n_cols, figsize = figsize);
        fig.suptitle(data_category, fontsize=20);
        
        # Use requested colour scheme
        if colourmap == "viridis":
            colours = cm.viridis(
                np.linspace(0, 1, len(col_labels))
            );

        # Create each subplot
        for index, row_label in enumerate(row_labels):
            # Get row and column of subplot
            row = index // n_cols;
            col = index % n_cols;

            # Get data to plot
            data = datas[row_label]
            
            # Draw bars and title
            axs[row, col].bar(col_labels, data, color=colours);
            axs[row, col].set_title(row_label);
            
            # Draw a mean line
            if mean_line_colour:
                mean = np.mean(data);
                axs[row, col].axhline(mean, color=mean_line_colour);

            # Draw confidence intervals around the mean
            if mean_line_colour and confidence_intervals_colour:
                # Calculate confidence intervals 
                lower_bound, upper_bound = self.__calculate_confidence_intervals(mean, data);
                # Draw confidence intervals
                axs[row, col].axhline(lower_bound, color=confidence_intervals_colour, linestyle="--");
                axs[row, col].axhline(upper_bound, color=confidence_intervals_colour, linestyle="--");
            
            # Disable column labels for all but the bottom row
            if row != n_rows - 1:
                axs[row, col].set_xticklabels([]);
            
            # Rotate labels
            else:
                axs[row, col].set_xticklabels(
                    col_labels, rotation=label_rotation, ha="right"
                );
        
        # Hide subplots beyond the number of rows
        for ax in axs.flat[len_row_labels:]:
            ax.axis("off");
            ax.set_visible(False);
        
        plt.tight_layout();

        # Avoid column labels being cut off and titles overlapping with plots
        plt.subplots_adjust(
            top = top,
            bottom = bottom, 
            hspace = hspace,
            wspace = wspace,
        );

        plt.show();

    """
    Calculate the confidence intervals around a given mean

    @param data: list of data to calculate confidence interval around
    @param alpha: significance level

    @return: tuple of lower and upper bounds of confidence interval
    """
    def __calculate_confidence_intervals(self, mean, data,
        alpha = 0.05
    ):
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

        return lower_bound, upper_bound;

    """
    Display multiple dataframes to the user
    @param frame: dictionary of titles and frameframes to display
    """
    def display_multiple_frames(self, frames):
        for title, frame in frames.items():
            print(title);
            self.display_frame(frame); 
            print("\n");

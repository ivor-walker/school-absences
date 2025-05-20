from views.baseview import BaseView;


import matplotlib.pyplot as plt;
import matplotlib.cm as cm;

import warnings;

import math;
import numpy as np;

"""
Class for getting input and displaying output to the user
"""
class TerminalView(BaseView):

    """
    Constructor: suppress all warnings
    """
    def __init__(self):
        # Suppress all warnings across all modules
        warnings.filterwarnings("ignore");

    """
    Print a line of text

    @param text: str, the text to self.display_line
    """
    def display_line(self, text):
        print(text);
    
    """
    Print a menu of options to the user

    @param options: dictionary of options to display
    """
    def display_menu(self, options):
        for key, value in options.items():
            self.display_line(f"{key}: {value}");

    """
    Prompt the user for input
    @param prompt: str, the prompt to display to the user
    @param type: str, the type of input to get from the user
    """
    def prompt_user(self,
        prompt = None, 
        type = "str",
        list_split_char = ",",
        year_split_char = "/",
    ):
        # Ask user to input entire list seperated by split char
        if type == "list": 
            prompt += f" (separated by '{list_split_char}'): ";
            return self.__prompt_for_list(prompt, list_split_char);
        
        # Ask user to input academic year
        elif type == "year":
            prompt = f"{prompt} (yyyy{year_split_char}yy, e.g 2007{year_split_char}08): ";
            return self.__prompt_for_year(prompt, split_char = year_split_char);

        elif type == "int":
            prompt += f" : ";
            return self.__prompt_for_int(prompt);
        
        elif type == "str":
            return self.__prompt_for_str(prompt);

    """
    Ask for and recieve list input from the user
    """
    def __prompt_for_list(self, prompt, split_char):
        user_input = input(prompt);
        
        try:
            # Remove whitespace in list
            user_input = user_input.split(split_char);
            user_input = [item.strip() for item in user_input];
            split_char.join(user_input);

            return user_input;

        except: 
            self.display_line("Invalid list.");
            return self.__prompt_for_list(prompt, split_char);
        
    """
    Ask for and recieve a int from the user
    """
    def __prompt_for_int(self, prompt):
        user_input = input(int_prompt);

        # Check user input is a int
        try:
            integer = int(user_input);
            return integer;

        except:
            self.display_line("Invalid integer.");
            return self.__prompt_for_int(prompt);
    
    """
    Ask for and recieve an n digit year from the user
    """
    def __prompt_for_year(self, prompt,
        split_char = None,
        year_length = 7,
    ):
        # Ask user to input year
        year = input(prompt);

        # Check year is n digits
        str_year = str(year);
        if len(str_year) != year_length:
            self.display_line("Invalid year.");
            return self.__prompt_for_year(prompt);

        return str_year.replace(split_char, ""); 

    """
    Ask for and recieve a string from the user
    """
    def __prompt_for_str(self, prompt):
        string = input(prompt);

        # Check string is not empty
        if not string:
            self.display_line("Invalid text.");
            return self.__prompt_for_str(prompt);

        return string;

    """
    Display a Spark dataframe to the user
    @param frame: frameframe to display
    """
    def display_frame(self, frame):
        # Show all columns in entirety
        frame.show(frame.count(), False);
       
    """
    Display multiple graphs

    @param datas: dictionary of data to display 
    @param title: title of figure
    @param n_cols: number of columns in grid of bar charts
    @param colourmap: colour scheme to use
    @param type: type of plot to display
    @param mean_line_colour: colour of mean line (None to disable)
    @param confidence_intervals_colour: colour of confidence intervals (None to disable)
    @param label_rotation: rotation of x-axis labels
    @param figsize: size of figure
    @param top: top margin of figure
    @param bottom: bottom margin of figure
    @param hspace: vertical spacing between subplots
    @param wspace: horizontal spacing between subplots
    """
    def display_graphs(self, datas,
        title = "",
        n_cols = 2,
        colourmap = "viridis",
        type = "bar",
        mean_line_colour = "red", 
        confidence_intervals_colour = "red",
        num_cols_for_cis = 5,
        label_rotation = 15,
        figsize = (15, 20),
        top = 0.93,
        bottom = 0.1, 
        hspace = 0.175,
        wspace = 0.08,
    ):
        # Extract column and row labels
        metadata = datas["metadata"];
        col_labels = metadata["col_labels"];
        index_labels = metadata["index_labels"];
        len_index_labels = len(index_labels);
        
        # Hide confidence intervals if not enough columns are being compared
        if len(col_labels) < num_cols_for_cis:
            confidence_intervals_colour = None;

        # Set up subplot grid, figure and title
        n_rows = math.ceil(
            len_index_labels / n_cols
        );
        fig, axs = plt.subplots(n_rows, n_cols, figsize = figsize);
        fig.suptitle(title, fontsize=20);
        
        # Use requested colour scheme
        colours = self.__get_colours(colourmap, len(col_labels)); 
        
        # Create each subplot
        for index, row_label in enumerate(index_labels):
            # Get row and column of subplot
            row = index // n_cols;
            col = index % n_cols;

            # Get data to plot
            data = datas[row_label];
            
            # Draw bar graph             
            if type == "bar":
                axs[row, col].bar(col_labels, data["data"], color=colours);
        
            # Set title
            axs[row, col].set_title(row_label);
            
            # Draw a mean line
            if mean_line_colour:
                axs[row, col].axhline(data["mean"], color=mean_line_colour);

            # Draw confidence intervals around the mean
            if mean_line_colour and confidence_intervals_colour:
                axs[row, col].axhspan(
                    data["lower_ci"],
                    data["upper_ci"],
                    color = confidence_intervals_colour,
                    alpha=0.5
                ); 

            # Disable column labels for all but the bottom row
            if row != n_rows - 1:
                axs[row, col].set_xticklabels([]);
            
            # Rotate labels
            else:
                axs[row, col].set_xticklabels(
                    col_labels, rotation=label_rotation, ha="right"
                );
        
        # Hide subplots beyond the number of rows
        for ax in axs.flat[len_index_labels:]:
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
    Get a list of colours for a colourmap
    
    @param colourmap: colour scheme to use
    @param n_colours: number of colours to get

    @return list of colours
    """
    def __get_colours(self, colourmap, n_colours):
        return cm.get_cmap(colourmap)(np.linspace(0, 1, n_colours));
    
    """
    Display a single graph
    """
    def display_single_graph(self, data, 
        title = "",
        type = "line",
        figsize = (15, 10),
        colourmap = "viridis",
        mean_line_colour = "red",
        confidence_intervals_colour = "red",
        num_cols_for_cis = 5,
        label_rotation = 15,
    ):
        # Extract column and row labels
        metadata = data["metadata"];
        col_labels = metadata["col_labels"];
        index_labels = metadata["index_labels"];

        # Set up figure and title
        fig, ax = plt.subplots(figsize = figsize);
        fig.suptitle(title, fontsize=20); 
        colours = self.__get_colours(colourmap, len(index_labels));
        
        # Enable confidence intervals only if enough columns or rows are being compared
        if len(col_labels) < num_cols_for_cis and len(row_labels) < num_cols_for_cis:
            confidence_intervals_colour = None;
            
        # Draw 2d line graph
        if type == "line":
            # Draw a line for each row 
            for index, row_label in enumerate(index_labels):
                ax.plot(
                    col_labels, 
                    data[row_label]["data"], 
                    label = row_label, 
                    color = colours[index]
                );

            # Draw a line for the mean
            if mean_line_colour:
                ax.plot(
                    col_labels,
                    metadata["col_means"],
                    label = "Mean",
                    color = mean_line_colour
                );

            if mean_line_colour and confidence_intervals_colour:
                ax.fill_between(
                    col_labels,
                    metadata["col_lower_cis"],
                    metadata["col_upper_cis"],
                    color = confidence_intervals_colour,
                    alpha = 0.5
                );
            
            # Set tickers as col labels and rotate
            ax.set_xticklabels(col_labels, rotation=label_rotation, ha="right");


        # Draw 1d bar graph
        elif type == "bar":
            # Draw a bar for each row
            for index, row_label in enumerate(index_labels):
                ax.bar(
                    row_label,
                    data[row_label]["data"],
                    label = row_label,
                    color = colours[index]
                );
            
            # Draw a horizontal line for the mean
            if mean_line_colour:
                ax.axhline(
                    metadata["col_means"],
                    label = "Mean",
                    color = mean_line_colour
                ); 
            
            # Shade confidence intervals around the mean line
            if mean_line_colour and confidence_intervals_colour:
                ax.axhspan(
                    metadata["col_lower_cis"][0],
                    metadata["col_upper_cis"][0],
                    color = confidence_intervals_colour,
                    alpha = 0.5
                );

            # Set tickers as index labels and rotate
            ax.set_xticklabels(index_labels, rotation=label_rotation, ha="right");

        # Add legend and rotate labels
        ax.legend();
        
        plt.show();

    """
    Display multiple dataframes to the user
    @param frame: dictionary of titles and frameframes to display
    """
    def display_multiple_frames(self, frames):
        for title, frame in frames.items():
            self.display_line(title);
            self.display_frame(frame); 
            self.display_line("\n");

import matplotlib.pyplot as plt;
import matplotlib.cm as cm;

import math;
import numpy as np;

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
    Ask for and recieve a 4 digit year from the user
    """
    def __prompt_for_year(self, prompt):
        # Ask user to input int
        year = self.__prompt_for_int(prompt);

        # Check year is 4 digits
        str_year = str(year);
        if len(str_year) != 4:
            print("Invalid year.");
            return self.__prompt_for_year(prompt);

        # Turn YYYY into YYYY/YY
        

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
        label_rotation = 10,
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
                axs[row, col].fill_between(
                    col_labels,
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
        
        # Avoid column labels being cut off and titles overlapping with plots
        plt.subplots_adjust(
            top = top,
            bottom = bottom, 
            hspace = hspace,
            wspace = wspace,
        );

        plt.tight_layout();
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
    ):
        # Extract column and row labels
        metadata = data["metadata"];
        col_labels = metadata["col_labels"];
        index_labels = metadata["index_labels"];

        # Set up figure and title
        fig, ax = plt.subplots(figsize = figsize);
        fig.suptitle(title, fontsize=20); 
        colours = self.__get_colours(colourmap, len(index_labels));
         
        # Draw line graph
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

            # Draw confidence intervals around the mean
            if mean_line_colour and confidence_intervals_colour:
                ax.fill_between(
                    col_labels,
                    metadata["col_lower_cis"],
                    metadata["col_upper_cis"],
                    color = confidence_intervals_colour,
                    alpha = 0.5
                );

        # Add legend and plot
        ax.legend();
        plt.show();

    """
    Display multiple dataframes to the user
    @param frame: dictionary of titles and frameframes to display
    """
    def display_multiple_frames(self, frames):
        for title, frame in frames.items():
            print(title);
            self.display_frame(frame); 
            print("\n");

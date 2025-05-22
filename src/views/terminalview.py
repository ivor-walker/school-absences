from views.baseview import BaseView;


import warnings;

import math;
import numpy as np;

from utils.typevalidation import convert_type;

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
    Print a menu of options to the user and allow user to input a choice

    @param options: dictionary of options to display
    """
    def display_menu(self, options,
        greeting = "MAIN MENU",
    ):
        self.display_line(f"\n{greeting}");
            
        for key, value in options.items():
            self.display_line(f"{key}: {value}");

        choice = self.__single_prompt(
            prompt = "Enter your choice: ",
            type = "str"
        );

        return choice;
    
    """
    Display all prompts to the user sequentially
    """
    def prompt_user(self,
        prompts = None,
        types = None,
    ):
        responses = [];

        for prompt, type in zip(prompts, types):
            # Display prompt and get user input
            response = self.__single_prompt(prompt, type);
            responses.append(response);

        return responses;
            
    """
    Display a single prompt for the user for input
    """
    def __single_prompt(self,
        prompt = None, 
        type = "str",
        list_split_char = ",",
        year_split_char = "/",
    ):
        # Give prompt-specific instructions 
        if type == "list": 
            prompt += f" (separated by '{list_split_char}'): ";
        
        elif type == "year":
            prompt = f"{prompt} (yyyy{year_split_char}yy, e.g 2007{year_split_char}08): ";
            
        elif type == "int":
            prompt += f" : ";
        
        elif type == "str":
            ();
        
        user_input = input(prompt);

        return convert_type(user_input, type,
            list_split_char = list_split_char,
            year_split_char = year_split_char
        );

    """ 
    Display a Spark dataframe to the user
    @param frame: frameframe to display
    """
    def display_frame(self, frame):
        # Show all columns in entirety
        frame.show(frame.count(), False);
       
    
    """
    Display a set of given figures
    """
    def display_figures(self, 
        frames = None,
        figures = None,
        titles = None,
    ):
        for fig, title in zip(figures, titles): 
            self.display_line(f"\n{title}");
            fig.show();
        
        self.display_multiple_frames(frames);

    """
    Display multiple dataframes to the user
    @param frame: dictionary of titles and frameframes to display
    """
    def display_multiple_frames(self, frames):
        for title, frame in frames.items():
            self.display_line(title);
            self.display_frame(frame); 
            self.display_line("\n");

    """
    Display a user error
    @param error: error, the error to display
    """
    def display_error(self, error):
        self.display_line(f"ERROR: {str(error)}");

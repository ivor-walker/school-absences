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
    Display multiple dataframes to the user
    @param frame: dictionary of titles and frameframes to display
    """
    def display_multiple_frames(self, frames):
        for title, frame in frames.items():
            print(title);
            self.display_frame(frame); 
            print("\n");

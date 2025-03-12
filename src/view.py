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
            # Ask user to input entire list seperated by split char
            list_prompt = f"{prompt} (separated by {split_char})"
            return input(list_prompt).split(split_char);

    """
    Display the data to the user
    @param data: dataframe to display
    """
    def display_data(self, data):
        data.show();

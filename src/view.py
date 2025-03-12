"""
Class for getting input and displaying output to the user
"""

class View:
    """
    Prompt the user for input
    @param prompt: str, the prompt to display to the user
    @param type: str, the type of input to get from the user
    """
    def prompt_user(
        prompt = prompt, 
        type = "str",
        split_char = ","
    ):
        if type == "list": 
            prompt = f"{prompt} (separated by {split_char})"
            return input(prompt).split(split_char);

    """
    Display the data to the user
    @param data: any, the data to display
    """
    def display_data(data):
        print(data);

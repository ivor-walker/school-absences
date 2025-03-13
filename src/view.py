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
            list_prompt = f"{prompt} (separated by {split_char})";
            user_input = input(list_prompt);

            # Remove whitespace in list
            user_input = user_input.split(split_char);
            user_input = [item.strip() for item in user_input];
            split_char.join(user_input);

            return user_input;
    """
    Display a dataframe to the user
    @param data: dataframe to display
    """
    def display_data(self, data):
        data.show();

    """
    Display multiple dataframes to the user
    @param data: dictionary of titles and dataframes to display
    """
    def display_multiple_data(self, datas):
        for title, data in datas.items():
            print(title);
            self.display_data(data); 
            print("\n");

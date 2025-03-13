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
    Display a dataframes to the user
    @param frame: frameframe to display
    """
    def display_frame(self, frame):
        frame.show();

    """
    Display multiple dataframes to the user
    @param frame: dictionary of titles and frameframes to display
    """
    def display_multiple_frames(self, frames):
        for title, frame in frames.items():
            print(title);
            self.display_frame(frame); 
            print("\n");

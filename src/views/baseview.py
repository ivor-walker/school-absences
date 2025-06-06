"""
View's interface
"""
class BaseView():
    def display_line(self, text):
        raise NotImplementedError;
    
    def display_menu(self, options):
        raise NotImplementedError;

    def prompt_user(self, prompt):
        raise NotImplementedError;

    def display_frame(self, frame):
        raise NotImplementedError;
    
    def display_multiple_frames(self, frames):
        raise NotImplementedError;

    def display_error(self, error):
        raise NotImplementedError;
        
    def display_graphs(self, dataset):
        raise NotImplementedError;
    

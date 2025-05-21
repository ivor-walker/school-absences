from controller import Controller;

"""
Entry point of the program for terminal view
"""
view_type = "terminal":
controller = Controller(view_type);

# Menu loop
while True:
    # Show menu and ask user for choice
    choice = controller.display_menu(controller.__menu);
                
    # Run corresponding function based on user choice
    try:
        if choice == "1": 
            controller.get_enrolment_by_la_over_time();
        elif choice == "2":
            controller.get_auth_by_school_type();
        elif choice == "3":
            controller.get_auth_by_school_type_detailed();
        elif choice == "4":
            controller.get_unauth_by_la_region();
        elif choice == "5":
            controller.compare_la_in_year();
        elif choice == "6":
            controller.compare_region_attendance_over_time();
        elif choice == "7":
            controller.eda_school_type_location_absences();
        elif choice == "8":
            controller.model_school_type_location_absences(display_results = True);
        elif choice == "9":
            controller.model_school_type_location_absences(display_detailed_results = True);
        elif choice == "0":
            break;
        else:
            raise ValueError("Invalid choice.");
    
    # Print any anticipated errors
    except ValueError as e:
        controller.display_error(f"Error: {e}");

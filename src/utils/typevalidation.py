"""
Check and convert a string to a target type
"""
def convert_type(target_value, target_type,
    list_split_char = ",",
    year_split_char = "/",
    year_len = 7
):
    # For a list, split into list and remove all whitespace in elements
    if target_type == "list":
        try:
            target_value = target_value.split(list_split_char);
        except Exception as e:
            raise ValueError(f"{target_value} is not a valid list");

        target_value = [value.strip() for value in target_value];

    elif target_type == "int":
        try:
            target_value = int(target_value);
        except Exception as e:
            raise ValueError(f"{target_value} is not a valid integer");

    elif target_type == "year":
        str_year = str(target_value);
        if len(str_year) != year_len:
            raise ValueError(f"{target_value} is not a valid year");
        
        target_value = target_value.replace(year_split_char, "");
    
    elif target_type == "string":
        if not target_value:
            raise ValueError(f"{target_value} is not a valid string");
    
    return target_value;

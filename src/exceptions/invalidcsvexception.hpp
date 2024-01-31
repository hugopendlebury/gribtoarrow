#pragma once

class  InvalidCSVException :  public std::runtime_error
{
public:

    InvalidCSVException(std::string filepath) : std::runtime_error("Invalid CSV file - is there a formatting error with file " + filepath) { }
 
};
#pragma once

class GribException :  public std::runtime_error
{
public:

    GribException(std::string errorDetails) : std::runtime_error("Exception " + errorDetails) { }
 
};
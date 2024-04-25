#pragma once

class CodesGetDoubleValuesAsArrayException :  public std::runtime_error
{
public:

    CodesGetDoubleValuesAsArrayException(std::string errorDetails) : std::runtime_error("Exception " + errorDetails) { }
 
};
#pragma once

class ArrowGenericException :  public std::runtime_error
{
public:

    ArrowGenericException(std::string errorDetails) : std::runtime_error("Exception " + errorDetails) { }
 
};
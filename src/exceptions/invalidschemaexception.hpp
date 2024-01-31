#pragma once

class  InvalidSchemaException :  public std::runtime_error
{
public:

    InvalidSchemaException(std::string errorMsg) : std::runtime_error("Invalid Schema " + errorMsg) { }
 
};
#pragma once

class UnableToCreateArrowTableReaderException :  public std::runtime_error
{
public:

    UnableToCreateArrowTableReaderException(std::string errorDetails) : std::runtime_error("Unable to create arrow table reader " + errorDetails) { }
 
};
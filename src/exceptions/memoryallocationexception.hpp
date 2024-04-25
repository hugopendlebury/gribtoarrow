#pragma once

class MemoryAllocationException :  public std::runtime_error
{
public:

    MemoryAllocationException(std::string errorDetails) : std::runtime_error("Exception " + errorDetails) { }
 
};
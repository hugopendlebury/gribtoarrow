#pragma once

class  NoSuchGribFileException :  public std::runtime_error
{
public:

    NoSuchGribFileException(std::string filepath) : std::runtime_error("Unable to find file " + filepath) { }
 
};
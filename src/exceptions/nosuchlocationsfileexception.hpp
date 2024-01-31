#pragma once

class  NoSuchLocationsFileException :  public std::runtime_error
{
public:

    NoSuchLocationsFileException(std::string filepath) : std::runtime_error("Unable to find Locations file " + filepath) { }
 
};
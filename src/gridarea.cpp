#include "gridarea.hpp"
#include <sstream>

ostream& operator<<(ostream &os, const GridArea &a) {
    return (os << "\n lat1:" << a.m_longitudeOfFirstPoint 
                << "\n lon1: " << a.m_longitudeOfFirstPoint 
                << "\n lat2: " << a.m_latitudeOfLastPoint  
                << "\n lon2: " << a.m_longitudeOfLastPoint  << std::endl);
}
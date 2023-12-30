#ifndef GRID_AREA_H_INCLUDED
#define GRID_AREA_H_INCLUDED

#include <functional>
#include "caster.hpp"


using namespace std;

class GridArea
{

    public:

        long m_latitudeOfFirstPoint;
        long m_longitudeOfFirstPoint;
        long m_latitudeOfLastPoint;
        long m_longitudeOfLastPoint;


        GridArea(long latitudeOfFirstPoint, 
                long longitudeOfFirstPoint,
                long latitudeOfLastPoint, 
                long longitudeOfLastPoint) : m_latitudeOfFirstPoint(latitudeOfFirstPoint),
                                             m_longitudeOfFirstPoint(longitudeOfFirstPoint),
                                             m_latitudeOfLastPoint(latitudeOfLastPoint),
                                             m_longitudeOfLastPoint(longitudeOfLastPoint) {}

        // Match all fields incase of collision
        bool operator==(const GridArea& other) const
        {
            return m_latitudeOfFirstPoint == other.m_latitudeOfFirstPoint 
                    && m_longitudeOfFirstPoint == other.m_longitudeOfFirstPoint
                    && m_latitudeOfLastPoint == other.m_latitudeOfLastPoint 
                    && m_longitudeOfLastPoint == other.m_longitudeOfLastPoint;;
        }

      
};

// Custom specialization of std::hash can be injected in namespace std.
template<>
struct hash<GridArea>
{
    size_t operator()(const GridArea& ga) const noexcept
    {
        unsigned long long h = (ga.m_latitudeOfFirstPoint << 24) | (ga.m_longitudeOfFirstPoint << 16) 
        | (ga.m_latitudeOfLastPoint << 8) | ga.m_longitudeOfLastPoint; 
        return h;
    }
}; 

#endif /*GRID_AREA_H_INCLUDED*/
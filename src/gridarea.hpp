#ifndef GRID_AREA_H_INCLUDED
#define GRID_AREA_H_INCLUDED

#include <functional>
#include "caster.hpp"
#include <sstream>


using namespace std;

class GridArea
{

    public:

        const double m_latitudeOfFirstPoint;
        const double m_longitudeOfFirstPoint;
        const double m_latitudeOfLastPoint;
        const double m_longitudeOfLastPoint;
        const bool m_iScansNegatively;
        const bool m_jScansPositively;
        const long m_numberOfPoints;
        friend ostream& operator<<(ostream &os, const  GridArea& a);


        GridArea(const double latitudeOfFirstPoint, 
                const double longitudeOfFirstPoint,
                const double latitudeOfLastPoint, 
                const double longitudeOfLastPoint,
                const bool iScansNegatively,
                const bool jScansPositively,
                const long numberOfPoints) : m_latitudeOfFirstPoint(latitudeOfFirstPoint),
                                             m_longitudeOfFirstPoint(longitudeOfFirstPoint),
                                             m_latitudeOfLastPoint(latitudeOfLastPoint),
                                             m_longitudeOfLastPoint(longitudeOfLastPoint),
                                             m_iScansNegatively(iScansNegatively),
                                             m_jScansPositively(jScansPositively),
                                             m_numberOfPoints(numberOfPoints) {}

        // Match all fields incase of collision
        bool operator==(const GridArea& other) const
        {
            return m_latitudeOfFirstPoint == other.m_latitudeOfFirstPoint 
                    && m_longitudeOfFirstPoint == other.m_longitudeOfFirstPoint
                    && m_latitudeOfLastPoint == other.m_latitudeOfLastPoint 
                    && m_longitudeOfLastPoint == other.m_longitudeOfLastPoint
                    && m_iScansNegatively == other.m_iScansNegatively
                    && m_jScansPositively == other.m_jScansPositively
                    && m_numberOfPoints == other.m_numberOfPoints;
        }
      
};


template<>
struct hash<GridArea>
{
    size_t operator()(const GridArea& ga) const noexcept
    {

        unsigned long long h = (ga.m_latitudeOfFirstPoint * ga.m_longitudeOfFirstPoint * ga.m_latitudeOfLastPoint 
                * ga.m_longitudeOfLastPoint) + (ga.m_numberOfPoints * (ga.m_iScansNegatively == 0 ? 10 : ga.m_iScansNegatively)
                * (ga.m_jScansPositively == 0 ? 10 : ga.m_jScansPositively)); 
        return h;
    }
}; 

#endif /*GRID_AREA_H_INCLUDED*/
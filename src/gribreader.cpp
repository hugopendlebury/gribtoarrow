#include <vector>
#include <algorithm>
#include <ranges>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include "arrowutils.hpp"
#include "caster.hpp"
#include "gridarea.hpp"
#include "gribreader.hpp"
#include "gribmessage.hpp"
#include "gribmessageiterator.hpp"
#include "caster.hpp"
#include "converter.hpp"

using namespace std;

GribReader::GribReader(string filepath) : filepath(filepath) {

    m_endMessage = new GribMessage(
                    this, 
                    NULL, 
                    -999l);
    fin = fopen(filepath.c_str(), "rb");
    if (!fin) {
        cout << "Error: unable to open input file" << filepath << endl;
    } else {
        cout << "I'm ready file is " << fin << endl;
        
    }
};

GribReader GribReader::withStations(std::shared_ptr<arrow::Table> stations) {
    //TODO - add some validation
    //the table should contain 2 columns "lat" and "lon"
    this->stations = stations.get();

    return *this;
}

enum conversionTypes {
    Add,
    Subtract,
    Multiply,
    Divide
};

GribReader GribReader::withConversions(std::shared_ptr<arrow::Table> conversions) {
    cout << "Setting conversions" << endl;
    this->conversions = conversions.get();

    auto rowConversion = ColumnarTableToVector(conversions);
    
    for (auto row : rowConversion.ValueOrDie()) {

        cout <<  "adding conversions" << endl ;

        vector<pair<conversionTypes, optional<double>>> methods {
                            make_pair(conversionTypes::Add, row.additionValue), 
                            make_pair(conversionTypes::Subtract, row.subtractionValue), 
                            make_pair(conversionTypes::Multiply, row.multiplicationValue), 
                            make_pair(conversionTypes::Divide , row.divisionValue)
        };

        auto match = ranges::find_if(methods, [](const pair<conversionTypes, optional<double>>& v) {
            return v.second.has_value();
        });

        if (match != methods.end()) {
            cout << "Adding to cache for " << row.parameterId << endl;
 
            std::function<arrow::Result<arrow::Datum>(arrow::Datum, arrow::Datum)> conversionFunc;

            cout << "JEFF" << endl;

            switch(match->first) {
                case conversionTypes::Add:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return arrow::compute::Add(lhs, rhs);
                    };
                    break;
                case conversionTypes::Subtract:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return arrow::compute::Subtract(lhs, rhs);
                    };
                    break;
                case conversionTypes::Multiply:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return arrow::compute::Multiply(lhs, rhs);
                    };
                    break;
                case conversionTypes::Divide:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return arrow::compute::Divide(lhs, rhs);
                    };
                    break;
            }

            auto converter = new Converter(conversionFunc, match->second.value());

            conversion_funcs.emplace(row.parameterId, converter);
             
        }
    }
    
    return *this;
}

optional<function<arrow::Result<shared_ptr<arrow::Array>>(shared_ptr<arrow::Array>)>> GribReader::getConversions(long parameterId) {
    auto match = conversion_funcs.find((int64_t) parameterId);
    if (match == conversion_funcs.end()) {
        return std::nullopt;
    } else {
        return std::optional{*match->second};
    }
}

Iterator GribReader::begin() { 
    codes_handle* h = codes_handle_new_from_file(0, fin, PRODUCT_GRIB, &err);
    auto m = new GribMessage(this, h, 0l);
    return Iterator(this, m, m_endMessage);
}

Iterator GribReader::end()   {
    return Iterator( this,  m_endMessage, m_endMessage );
} 

std::optional<GribLocationData*> GribReader::getLocationDataFromCache(std::unique_ptr<GridArea>& area) {

    //check the cache
    auto ga = *area.get();
    auto cache_result = location_cache.find(ga);
    return cache_result != location_cache.end() ? std::optional<GribLocationData*> {cache_result->second}
                                         : std::nullopt;
}

GribLocationData* GribReader::addLocationDataToCache(std::unique_ptr<GridArea>& area, GribLocationData* locationData) {

    auto numberOfPoints = locationData->numberOfPoints;
    auto a = *area.get();
    location_cache.emplace(a, locationData);
    return locationData;

}

arrow::Table* GribReader::getStations(std::unique_ptr<GridArea>& area) {

    auto ga = *area.get();

    if (auto search = stations_in_area.find(ga); search != stations_in_area.end()) {
            std::cout << "Found area" << std::endl;
    }
    else {
            std::cout << "No area found will add areas\n";
            //TODO compute which stations are in the area
            stations_in_area.emplace(std::make_pair(ga, stations));
            std::cout << "Added stations\n";
    }

    auto required_stations = stations_in_area.find(ga);
    auto matched =  stations_in_area[ga];
    return matched;
}

bool GribReader::hasStations() {
    return stations != NULLPTR;
}
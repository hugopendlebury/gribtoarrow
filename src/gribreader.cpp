#include <vector>
#include <algorithm>
//#include <ranges>
#include <arrow/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/compute/api.h>

#include "arrow/dataset/file_ipc.h"
#include "arrow/table.h"

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/compute/expression.h>
#include <arrow/csv/api.h>
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
namespace cp = arrow::compute;
namespace ad = arrow::dataset;

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

GribReader GribReader::withLocations(std::shared_ptr<arrow::Table> locations) {
    //TODO - add some validation
    //the table should contain 2 columns "lat" and "lon"
    this->shared_locations = locations;
    return *this;
}

GribReader GribReader::withLocations(std::string path){

    //Reads a CSV  with the location data and enriches it with a row_number / surrogate_key
    //TODO add validation of the column names

    std::shared_ptr<arrow::Table> locations = getTableFromCsv(path);

    //Append an additional column to the table called surrogate key
    auto numberOfRows = locations.get()->num_rows();
    auto surrogate_columns = createSurrogateKeyCol(numberOfRows);
    auto skField = arrow::field("surrogate_key", arrow::uint16());
    auto chunkedArray = std::make_shared<arrow::ChunkedArray>(arrow::ChunkedArray(surrogate_columns.ValueOrDie()));
    locations = locations.get()->AddColumn(0, skField, chunkedArray).ValueOrDie();


    this->shared_locations = locations;
    return *this;
}

enum conversionMethods {
    Add,
    Subtract,
    Multiply,
    Divide
};

enum conversionDataTypes {
    String,
    Float
};


GribReader GribReader::withConversions(std::string conversionsPath) {
    cout << "Reading conversions CSV" << endl;
    //TODO add validation of the column names
    std::shared_ptr<arrow::Table> conversions = getTableFromCsv(conversionsPath);

    std::unordered_map<std::string, std::string> requiredFields;
    requiredFields.emplace(make_pair("parameterId", "int64" ));
    requiredFields.emplace(make_pair("addition_value", "double" ));
    requiredFields.emplace(make_pair("subtraction_value", "double" ));
    requiredFields.emplace(make_pair("multiplication_value", "double" ));
    requiredFields.emplace(make_pair("division_value", "double" ));
    requiredFields.emplace(make_pair("ceiling_value", "double" ));

    auto conversionsCSVTable = conversions.get();

    int colIndex = 0;
    for (auto field : conversionsCSVTable->fields()) {
        auto colName = field->name();
        auto it = requiredFields.find(colName);
        if(it == requiredFields.end()){
            //Column not found should let python know
            std::cout << "Column " << colName << " was not found in CSV" << std::endl;
            //TODO
            //We should throw an exceptions here

        } else {
            std::cout << "Column " << colName << " was found in CSV" << std::endl;
            auto required_type = it->second;
            auto type = field.get()->type();
            
            //Surely there is an emum or cleaner way ?
            if (type.get()->name() != required_type) {
                std::cout << "type of column " << colName <<" does not match expected type recieved type  " << type->name() << std::endl;
                
            }
        }
        ++colIndex;
    }

    //withConversions(conversions);
    return *this;
}

GribReader GribReader::withConversions(std::shared_ptr<arrow::Table> conversions) {
    cout << "Setting conversions" << endl;
    this->conversions = conversions.get();

    auto rowConversion = ColumnarTableToVector(conversions);
    
    for (auto row : rowConversion.ValueOrDie()) {

        cout <<  "adding conversions" << endl ;

        vector<pair<conversionMethods, optional<double>>> methods {
                            make_pair(conversionMethods::Add, row.additionValue), 
                            make_pair(conversionMethods::Subtract, row.subtractionValue), 
                            make_pair(conversionMethods::Multiply, row.multiplicationValue), 
                            make_pair(conversionMethods::Divide , row.divisionValue)
        };

        pair<conversionMethods, optional<double>> firstMatch;
        bool match = false;
        for(auto row: methods) {
            if(row.second.has_value()) {
                match = true;
                firstMatch = row;
            }

        }

        //REMOVED THIS - Only present in newer compilers
        //auto match = ranges::find_if(methods, [](const pair<conversionTypes, optional<double>>& v) {
        //    return v.second.has_value();
        //});

        if (match) {
            cout << "Adding to cache for " << row.parameterId << endl;
 
            std::function<arrow::Result<arrow::Datum>(arrow::Datum, arrow::Datum)> conversionFunc;

            switch(firstMatch.first) {
                case conversionMethods::Add:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return cp::Add(lhs, rhs);
                    };
                    break;
                case conversionMethods::Subtract:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return cp::Subtract(lhs, rhs);
                    };
                    break;
                case conversionMethods::Multiply:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return cp::Multiply(lhs, rhs);
                    };
                    break;
                case conversionMethods::Divide:
                    conversionFunc = [](arrow::Datum lhs, arrow::Datum rhs) {
                        return cp::Divide(lhs, rhs);
                    };
                    break;
            }

            auto converter = new Converter(conversionFunc, firstMatch.second.value());

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

std::shared_ptr<arrow::Table> GribReader::getLocations(std::unique_ptr<GridArea>& area) {

    auto ga = *area.get();

    if (auto search = locations_in_area.find(ga); search != locations_in_area.end()) {
            std::cout << "Found area" << std::endl;
    }
    else {
            auto latDirection = ga.m_jScansPositively;   
            auto lonDirection = ga.m_iScansNegatively;         

            cp::Expression c1, c2, c3, c4;

            if(latDirection) {
                c1 = cp::greater_equal(cp::field_ref("lat"), cp::literal(ga.m_latitudeOfFirstPoint));
                c2 = cp::less_equal(cp::field_ref("lat"), cp::literal(ga.m_latitudeOfLastPoint));
            } else {
                c1 = cp::less_equal(cp::field_ref("lat"), cp::literal(ga.m_latitudeOfFirstPoint));
                c2 = cp::greater_equal(cp::field_ref("lat"), cp::literal(ga.m_latitudeOfLastPoint));
            }
            if(lonDirection) {
                c3 = cp::less_equal(cp::field_ref("lon"), cp::literal(ga.m_longitudeOfFirstPoint));
                c4 = cp::greater_equal(cp::field_ref("lon"), cp::literal(ga.m_longitudeOfLastPoint));
            } else {
                c3 = cp::greater_equal(cp::field_ref("lon"), cp::literal(ga.m_longitudeOfFirstPoint));
                c4 = cp::less_equal(cp::field_ref("lon"), cp::literal(ga.m_longitudeOfLastPoint));
            }

            auto filterCondition = cp::and_({c1,c2,c3,c4});

            // Wrap the Table in a Dataset so we can use a Scanner
            std::shared_ptr<arrow::dataset::Dataset> dataset =
                    std::make_shared<arrow::dataset::InMemoryDataset>(shared_locations);

            auto options = std::make_shared<arrow::dataset::ScanOptions>();
            options->filter = filterCondition;
            
            auto builder = arrow::dataset::ScannerBuilder(dataset, options);
            auto scanner = builder.Finish();

            if (scanner.ok()) {
            // Perform the Scan and make a Table with the result
                auto result = scanner.ValueUnsafe()->ToTable();
                cout << "Successfully filtered location table" << endl;
                auto filteredResults = result.ValueOrDie();
                cout << "Filtered table has "<< filteredResults.get()->num_rows() << " rows" << endl;
                locations_in_area.emplace(std::make_pair(ga, filteredResults));
                std::cout << "Added locations to cache\n";
            } else {
                cout << "OH NO" << endl;
            }
            
    }

    auto required_locations = locations_in_area.find(ga);
    auto matched =  locations_in_area[ga];
    return matched;
}

bool GribReader::hasLocations() {
    return shared_locations.use_count() > 0;
}

std::shared_ptr<arrow::Table> GribReader::getTableFromCsv(std::string path){
    std::shared_ptr<arrow::io::ReadableFile> infile = arrow::io::ReadableFile::Open(path).ValueOrDie();
    auto csv_reader =
        arrow::csv::TableReader::Make(
            arrow::io::default_io_context(), infile, arrow::csv::ReadOptions::Defaults(),
            arrow::csv::ParseOptions::Defaults(), arrow::csv::ConvertOptions::Defaults()).ValueOrDie();
    
    std::shared_ptr<arrow::Table> table = csv_reader->Read().ValueOrDie();

    return table;
}

arrow::Result<std::shared_ptr<arrow::Array>> GribReader::createSurrogateKeyCol(long numberOfRows){

    vector<uint16_t> row_ids;
    for(auto i =0 ; i <  numberOfRows; ++i) {
        row_ids.emplace_back(i);
    }

    arrow::UInt16Builder surrogateBuilder;
    ARROW_RETURN_NOT_OK(surrogateBuilder.AppendValues(row_ids));
    std::shared_ptr<arrow::Array> arrayValues;
    ARROW_ASSIGN_OR_RAISE(arrayValues,surrogateBuilder.Finish());
    return arrayValues;

 }

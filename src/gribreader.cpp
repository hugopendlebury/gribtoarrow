#include <vector>
#include <algorithm>
//#include <ranges>
#include <arrow/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/compute/api.h>

#include "arrow/dataset/file_ipc.h"
#include "arrow/table.h"
#include <set>
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
#include "exceptions/nosuchgribfileexception.hpp"
#include "exceptions/nosuchlocationsfileexception.hpp"
#include "exceptions/arrowtablereadercreationexception.hpp"
#include "exceptions/arrowgenericexception.hpp"
#include "exceptions/invalidcsvexception.hpp"
#include "exceptions/invalidschemaexception.hpp"

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
        throw NoSuchGribFileException(filepath);
        cout << "Error: unable to open input file" << filepath << endl;
    } else {
        cout << "I'm ready file is " << fin << endl;
        
    }
};

GribReader GribReader::withLocations(std::shared_ptr<arrow::Table> locations) {
    //TODO - add some validation
    this->shared_locations = locations;
    return *this;
}

GribReader GribReader::withRepeatableIterator(bool repeatable) {
    //TODO - add some validation
    this->isRepeatable = repeatable;
    return *this;
}

void GribReader::validateConversionFields(std::shared_ptr<arrow::Table> locations, std::string table_name) {
    auto table = locations.get();
    auto columns = table->ColumnNames();
    std::set<std::string> columnsSet(std::make_move_iterator(columns.begin()),
              std::make_move_iterator(columns.end()));


    std::vector<std::string> required_columns = {"parameterId", 
                                        "addition_value", 
                                        "subtraction_value",
                                        "multiplication_value",
                                        "division_value",
                                        "ceiling_value"};

    for (auto col : required_columns) {
        const bool is_in = columnsSet.find(col) != columnsSet.end();
        if (!is_in){
            std::string errDetail = "Column " + col + " is not present in schema of table " + table_name;
            throw InvalidSchemaException(errDetail);
        }
    }
}

void GribReader::castConversionFields(std::shared_ptr<arrow::Table> locations, std::string table_name) {
    auto table = locations.get();

    std::vector<std::string> f64_cols = {"addition_value", 
                                        "subtraction_value",
                                        "multiplication_value",
                                        "division_value",
                                        "ceiling_value"};
    
    for (auto colName: f64_cols) {
        auto col = table->GetColumnByName(colName).get();

        cp::CastOptions castOptions;
        castOptions.to_type = arrow::float64().get();

        auto x = arrow::float64().get();
        for (auto chunk : col->chunks()) {
            auto arr = chunk.get();
            auto result = cp::Cast(*arr, castOptions.to_type, castOptions);
            if (result.ok()) {
                auto converted = result.ValueOrDie();
                //Ok it's converted do we need to update the table ?
            }
        }
    }

}

GribReader GribReader::withLocations(std::string path){

    //Reads a CSV  with the location data and enriches it with a row_number / surrogate_key


    std::shared_ptr<arrow::Table> locations = getTableFromCsv(path, arrow::csv::ConvertOptions::Defaults());

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

    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> fieldTypes;
    fieldTypes.emplace(make_pair("parameterId", arrow::int64()));
    fieldTypes.emplace(make_pair("addition_value", arrow::float64()));
    fieldTypes.emplace(make_pair("subtraction_value", arrow::float64()));
    fieldTypes.emplace(make_pair("multiplication_value", arrow::float64()));
    fieldTypes.emplace(make_pair("division_value", arrow::float64()));
    fieldTypes.emplace(make_pair("ceiling_value", arrow::float64()));


    auto convertOptions = arrow::csv::ConvertOptions::Defaults();
    convertOptions.column_types = fieldTypes;

    std::shared_ptr<arrow::Table> conversions = getTableFromCsv(conversionsPath, convertOptions);
    validateConversionFields(conversions, " passed conversions via arrow");
    withConversions(conversions);
    return *this;
}

GribReader GribReader::withConversions(std::shared_ptr<arrow::Table> conversions) {
    cout << "Setting conversions" << endl;
    this->conversions = conversions.get();


    //the table should contain 2 columns "lat" and "lon"
    validateConversionFields(conversions, " passed conversions via arrow");

    //TODO - Validate types ?

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

    if (isRepeatable) {
        //TODO Make the iterator repeatable
        //Not sure how yet need to play with how this iterfaces at the python __iter__, next() and StopIteration level
    }
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

std::shared_ptr<arrow::Table> GribReader::getTableFromCsv(std::string path, arrow::csv::ConvertOptions convertOptions){
    auto infile = arrow::io::ReadableFile::Open(path);

    if (infile.ok()) {

        auto csv_reader =
            arrow::csv::TableReader::Make(
            arrow::io::default_io_context(), infile.ValueOrDie(), arrow::csv::ReadOptions::Defaults(),
            arrow::csv::ParseOptions::Defaults(), convertOptions);
            
        if(csv_reader.ok()) {
            auto table = csv_reader.ValueOrDie()->Read();
            
            if (table.ok()) {
                return table.ValueOrDie();
            } else {
                std::string errDetails = "Error reading results into arrow table is this a valid CSV ? ";
                throw InvalidCSVException(errDetails );
            }

        } else {
            std::string errDetails = "Unable to create arrow CSV table reader for file " + path;
            throw UnableToCreateArrowTableReaderException(errDetails);
        }

    


    } else {
        throw NoSuchLocationsFileException(path);
    }
    


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

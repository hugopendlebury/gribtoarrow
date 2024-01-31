#include <exception>
#include <string>

using namespace std;

class NoSuchFileException : public std::exception {

    public:

        NoSuchFileException(string filepath) : filepath(filepath) {
            std::string errType =  "Unable to find file ";
            errMsg = (errType + filepath).c_str();
        };

        const char* what() const noexcept {
            return errMsg;
        }
    
    private:
        std::string filepath;
        const char* errMsg;
};
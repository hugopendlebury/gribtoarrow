#include "eccodes.h"
#include <iterator>
#include <memory>
#include <utility>
#include <iterator> // For std::forward_iterator_tag
#include <cstddef>  // For std::ptrdiff_t
#include "gribmessage.hpp"
#include "gribmessageiterator.hpp"
#include "caster.hpp"


Iterator::Iterator(GribReader* reader, 
                    GribMessage* currentMessage, 
                    GribMessage* lastMessage) : 
                        reader(reader),
                        currentMessage(currentMessage),
                        m_lastMessage(lastMessage) {
                            m_ptr = currentMessage;
}


// Prefix increment
Iterator& Iterator::operator++() { 
    delete m_ptr;
    codes_handle* h = codes_handle_new_from_file(0, reader->fin, PRODUCT_GRIB, &err);
    if (h == NULL) {
        m_ptr = m_lastMessage;
        reader->setExhausted(true);
    } else {
        message_id++;
        auto m = new GribMessage(
                reader, 
                h, 
                message_id
                );
        m_ptr = m;
    }
    return *this; 
}  

// Postfix increment
Iterator Iterator::operator++(int) { 
    printf("\nPOSTFIX\n");
    Iterator tmp = *this; 
    ++(*this); 
    return tmp; 
}

 


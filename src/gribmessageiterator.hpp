#ifndef GRIB_MESSAGE_ITERATOR_H_INCLUDED
#define GRIB_MESSAGE_ITERATOR_H_INCLUDED

#include "eccodes.h"
#include <iterator>
#include <memory>
#include <utility>
#include <iterator> // For std::forward_iterator_tag
#include <cstddef>  // For std::ptrdiff_t
#include "gribmessage.hpp"
#include "caster.hpp"

class GribMessage;
class GribReader;

class Iterator { 

    public:

    using iterator_category = std::forward_iterator_tag;
    using difference_type   = std::ptrdiff_t;
    using value_type        = GribMessage;
    using pointer           = GribMessage*;  // or also value_type*
    using reference         = GribMessage&;  // or also value_type&

    Iterator(GribReader* reader, GribMessage* currentMessage, GribMessage* lastMessage);

    reference operator*() const { 
        return *m_ptr; 
    }
    pointer operator->() { 
        return m_ptr; 
    }

    // Prefix increment
    Iterator& operator++(); 

    // Postfix increment
    Iterator operator++(int) ;

    friend bool operator== (const Iterator& a, const Iterator& b) { return a.m_ptr == b.m_ptr; };
    friend bool operator!= (const Iterator& a, const Iterator& b) { return a.m_ptr != b.m_ptr; };       


    private:
        pointer m_ptr;
        int err              = 0;
        long message_id      = 0;
        GribReader*         reader;
        GribMessage*        currentMessage;
        GribMessage*        m_lastMessage;
};

#endif /* GRIB_MESSAGE_ITERATOR_H_INCLUDED */
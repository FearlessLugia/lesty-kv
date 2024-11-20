//
// Created by Kiiro Huang on 24-11-19.
//

#ifndef LOG_H
#define LOG_H

#include <iostream>

// log macro
// If NDEBUG is defined (release mode), then LOG(x) will be replaced with nothing
#ifdef NDEBUG
    #define LOG(x)
#else
    #define LOG(x) std::cout << x << std::endl
#endif

#endif //LOG_H

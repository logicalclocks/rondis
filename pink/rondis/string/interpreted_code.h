#include <ndbapi/NdbApi.hpp>

#ifndef STRING_INTERPRETED_CODE_H
#define STRING_INTERPRETED_CODE_H

#define RONDB_INSERT 2
#define RONDB_UPDATE 1
#define REG0 0
#define REG1 1
#define REG2 2
#define REG3 3
#define REG4 4
#define REG5 5
#define REG6 6
#define REG7 7
#define LABEL0 0
#define LABEL1 1

#define MEMORY_OFFSET_START 0
#define MEMORY_OFFSET_LEN_BYTES 4
#define MEMORY_OFFSET_STRING 6
#define NUM_LEN_BYTES 2
#define INCREMENT_VALUE 1
#define OUTPUT_INDEX 0
#define RONDB_KEY_NOT_NULL_ERROR 6000

#define INITIAL_INT_VALUE 1
#define INITIAL_INT_STRING '1'
#define INITIAL_INT_STRING_LEN 1
#define INITIAL_INT_STRING_LEN_WITH_LEN_BYTES 3

int initNdbCodeIncr(std::string *response,
                    NdbInterpretedCode *code,
                    const NdbDictionary::Table *tab);

#endif

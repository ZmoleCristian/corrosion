#include "deps/sqlite3ext.h"

/* Stable names regardless of libclang's naming of nested SQLite structs. */
typedef __typeof__(((sqlite3_index_info *)0)->aConstraint[0]) crsql_index_constraint;
typedef __typeof__(((sqlite3_index_info *)0)->aConstraintUsage[0]) crsql_index_constraint_usage;

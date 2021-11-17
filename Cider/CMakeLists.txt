set(SRC_FILE CiderExecutionKernel.cpp)
add_library(cider SHARED ${SRC_FILE})
target_link_libraries(cider DBEngine)
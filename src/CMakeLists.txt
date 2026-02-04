add_executable(main main.cpp)
target_link_libraries(main PRIVATE CURL::libcurl)

target_sources(main PRIVATE main.cpp)

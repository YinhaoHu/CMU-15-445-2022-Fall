if(EXISTS "/home/hoo/project/bustub/cmake-build-debug/test/lru_k_replacer_test[1]_tests.cmake")
  include("/home/hoo/project/bustub/cmake-build-debug/test/lru_k_replacer_test[1]_tests.cmake")
else()
  add_test(lru_k_replacer_test_NOT_BUILT lru_k_replacer_test_NOT_BUILT)
endif()

if(EXISTS "/home/hoo/project/bustub/cmake-build-debug/test/b_plus_tree_concurrent_test[1]_tests.cmake")
  include("/home/hoo/project/bustub/cmake-build-debug/test/b_plus_tree_concurrent_test[1]_tests.cmake")
else()
  add_test(b_plus_tree_concurrent_test_NOT_BUILT b_plus_tree_concurrent_test_NOT_BUILT)
endif()
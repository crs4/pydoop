C++ word count implementation, mostly for comparison purposes. Not run
together with other examples and/or tests by default. Includes the C++
pipes source so that we can just build and link everything together
into the executable task implementation.

Requirements: openssl dev version (e.g., yum install openssl-devel).

NOTE: the map function splits input values on space chars, unlike the
Java and Python versions, which split on multiple whitespace chars. This
can lead to a slightly different output, depending on the input text.

// BEGIN_COPYRIGHT
// END_COPYRIGHT
#ifndef PYDOOP_UTILS_HPP
#define PYDOOP_UTILS_HPP


// Convert (Python) dictionary to (C++) map. Taken from:
// https://galen.dulci.duhs.duke.edu/msi/doxygen/c%2B%2B/html/cppmap__conv__pif_8cpp-source.html

template<typename S, typename T> map<S, T> pydict2cppmap(PyObject* obj) {    
  map<S, T> cppmap;
  S key;
  T val;
  boost::python::dict pydict(boost::python::borrowed(obj));
  boost::python::list keylst = pydict.keys();
  int keylstlen = extract<int>(keylst.attr("__len__")());
  for(int i=0; i<keylstlen; i++) {
    key = extract<S>(keylst[i]);
    // WARNING; This fails if a default-constructed object of T
    // cannot be assigned to by arbitary objects of T.
    // See specialization for blitz Array.
    val = extract<T>(pydict[keylst[i]]);
    cppmap.insert(std::make_pair(key, val));
  }
  return cppmap;
}

#endif // PYDOOP_UTILS_HPP

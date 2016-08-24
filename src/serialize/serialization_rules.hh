/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2016 CRS4.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * END_COPYRIGHT
 */
#ifndef PYDOOP_SERIALIZE_SERIALIZATION_RULES_HH
#define PYDOOP_SERIALIZE_SERIALIZATION_RULES_HH

#include <Python.h>
#include <structmember.h> // Python
#include <errno.h>

#include <vector>
#include <map>
#include <string>

class WritableRules {
public:
  typedef std::vector<std::pair<std::string, std::string>> details_t;
  typedef std::map<const PyObject*, const details_t*> type_to_details_t;
  
public:
  WritableRules() {}
  ~WritableRules() {
    clear();
  }
  inline void add(const PyObject* otype, const details_t* cvect) {
    _t_2_details.insert(type_to_details_t::value_type(otype, cvect));
  }
  inline const details_t* get_rule(const PyObject* otype) const {
    return _t_2_details.at(otype);
  }
  inline bool has_rule(const PyObject* otype) const {
    return _t_2_details.find(otype) != _t_2_details.end();
  }
  inline void clear(void) {
    for(type_to_details_t::iterator it = _t_2_details.begin();
        it != _t_2_details.end(); ++it) {
      delete it->second;
    }
    _t_2_details.clear();
  }

private:
  type_to_details_t _t_2_details;
};

typedef struct {
  PyObject_HEAD
  WritableRules* rules;
} WritableRulesInfo;


PyObject* WritableRules_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int WritableRules_init(WritableRulesInfo *self, PyObject *args, PyObject *kwds);
void WritableRules_dealloc(WritableRulesInfo *self);


PyObject* WritableRules_add(WritableRulesInfo *self,  PyObject *args);
PyObject* WritableRules_rule(WritableRulesInfo *self, PyObject *type_name);


#endif //  PYDOOP_SERIALIZE_SERIALIZATION_RULES_HH

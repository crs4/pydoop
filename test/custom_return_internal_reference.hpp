// File: custom_return_internal_reference.hpp
// Description:
/// custom_call_policies::return_registered_internal_reference
///   Customised version of return_internal_reference which allows delegation
///   to some externally defined "MakeHolder" class
///   (defined as "A class whose static execute() creates an instance_holder")

#ifndef CUSTOM_RETURN_INTERNAL_REFERENCE_HPP
#define CUSTOM_RETURN_INTERNAL_REFERENCE_HPP

# include <boost/python/default_call_policies.hpp>
# include <boost/python/return_internal_reference.hpp>
# include <boost/python/reference_existing_object.hpp>
# include <boost/python/to_python_indirect.hpp>
# include <boost/mpl/if.hpp>
# include <boost/type_traits/is_same.hpp>


namespace custom_call_policies
{
namespace detail
{
using namespace boost::python;

/// Default "MakeHolder" model based on
/// "boost/python/to_python_indirect.hpp - detail::make_reference_holder" and
/// "boost/python/to_python_value.hpp - detail::registry_to_python_value"
struct make_registered_reference_holder
{
    /// Turns a pointer to a C++ type "T" into a "PyObject *" using registered
    /// type lookup. This means C++ type must be manually registered for
    /// conversion
    ///  @param T  Parameterised C++ type to convert
    ///  @param p  Pointer to instance of C++ type to convert
    ///  @return Python object built from registered conversion code
    template <class T>
    static PyObject* execute(T* p)
    {
        typedef objects::pointer_holder<T*, T> holder_t;
        T* q = const_cast<T*>(p);
        // Jump into conversion lookup mechanism
        typedef T argument_type;
        typedef converter::registered<argument_type> r;
    # if BOOST_WORKAROUND(__GNUC__, < 3)
        // suppresses an ICE, somehow
        (void)r::converters;
    # endif 
        return converter::registered<argument_type>::converters.to_python(q);
    }
};

/// reference_existing_object replacement allowing use of different
/// "MakeHolder" model.
///  @param  MakeReferenceHolderSubstitute - Class modelling "MakeHolder"
///                                         Defaults to
///                                         make_registered_reference_holder
template <class MakeReferenceHolderSubstitute>
struct subst_reference_existing_object :
  boost::python::reference_existing_object
{
    /// Implicitly relies on "detail" namespace implementation, and falls back
    /// on that implementation if it changes
    template <class T>
    struct apply
    {
      private:
        typedef typename reference_existing_object::apply<T>::type basetype_;
      public:
        typedef typename boost::mpl::if_<
          boost::is_same<basetype_
                        ,boost::python::to_python_indirect<
                          T, boost::python::detail::make_reference_holder> >
         ,boost::python::to_python_indirect<T, MakeReferenceHolderSubstitute>
         ,basetype_
        >::type type;
    };
};

/// return_internal_reference replacement allowing use of different
/// "ResultConverterGenerator" model rather than "reference_existing_object"
/// Falls back on Boost implementation if it ceases to use
/// "reference_existing_object"
///  @param ReferenceExistingObjectSubstitute  - "ResultConverterGenerator"
///                                             model replacement for
///                                             "reference_existing_object"
///  @param  owner_arg                         - See boost documentation
///  @param  BasePolicy_                       - See boost documentation
template <class ReferenceExistingObjectSubstitute
         ,std::size_t owner_arg = 1, class BasePolicy_ = default_call_policies>
struct subst_return_internal_reference :
  boost::python::return_internal_reference<owner_arg, BasePolicy_>
{
  private:
    typedef boost::python::return_internal_reference<owner_arg
                                                    ,BasePolicy_> basetype_;
  public:
    typedef typename boost::mpl::if_<
      boost::is_same<typename basetype_::result_converter
                    ,boost::python::reference_existing_object>
     ,ReferenceExistingObjectSubstitute
     ,typename basetype_::result_converter>::type result_converter;
};
} // Ends namespace detail


// Typedefs for programmer convenience
typedef detail::subst_reference_existing_object<
  detail::make_registered_reference_holder
  > reference_registered_existing_object;

// In place of a typedef template
/// Call policy to create internal references to registered types



//template <std::size_t owner_arg = 1,class BasePolicy_=default_call_policies>
template <std::size_t owner_arg,class BasePolicy_>
struct return_registered_internal_reference :
  detail::subst_return_internal_reference<reference_registered_existing_object
                                         ,owner_arg, BasePolicy_>
{};
} // Ends namespace custom_call_policies


#endif

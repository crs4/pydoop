#ifndef HACKED_WRAPPER_HPP
#define HACKED_WRAPPER_HPP

// Disable original implementation of wrapper.
#define WRAPPER_BASE_DWA2004722_HPP
#define WRAPPER_DWA2004720_HPP

# include <boost/python/detail/prefix.hpp>
# include <boost/type_traits/is_polymorphic.hpp>
# include <boost/mpl/bool.hpp>


namespace boost { namespace python {

    class override;

    namespace detail
    {
      class BOOST_PYTHON_DECL_FORWARD wrapper_base;
  
      namespace wrapper_base_ // ADL disabler
      {
	inline PyObject* get_owner(wrapper_base const volatile& w);

	inline PyObject*
	owner_impl(void const volatile* /*x*/, mpl::false_)
	{
	  return 0;
	}
    
	template <class T>
	inline PyObject*
	owner_impl(T const volatile* x, mpl::true_);
    
	template <class T>
	inline PyObject*
	owner(T const volatile* x)
	{
	  return wrapper_base_::owner_impl(x,is_polymorphic<T>());
	}
      }
  
      class BOOST_PYTHON_DECL wrapper_base
      {
	friend void initialize_wrapper(PyObject* self, wrapper_base* w);
	friend PyObject* wrapper_base_::get_owner(wrapper_base const volatile& w);
      protected:
	wrapper_base() : m_self(0) {}
          
	override get_override(char const* name, PyTypeObject* class_object) const;

      private:
	void detach();
      
      protected:
	PyObject* m_self;
      };

      namespace wrapper_base_ // ADL disabler
      {
	template <class T>
	inline PyObject*
	owner_impl(T const volatile* x, mpl::true_)
	{
	  if (wrapper_base const volatile* w = dynamic_cast<wrapper_base const volatile*>(x))
	    {
	      return wrapper_base_::get_owner(*w);
	    }
	  return 0;
	}
    
	inline PyObject* get_owner(wrapper_base const volatile& w)
	{
	  return w.m_self;
	}
      }
  
      inline void initialize_wrapper(PyObject* self, wrapper_base* w)
      {
	w->m_self = self;
      }

      inline void initialize_wrapper(PyObject* /*self*/, ...) {}

  
    } // namespace detail
  } // namespace python
} // namespace boost


# include <boost/python/override.hpp>
# include <boost/python/converter/registered.hpp>
# include <boost/python/detail/sfinae.hpp>

namespace boost { namespace python { 

    template <class T>
    class wrapper : public detail::wrapper_base
    {
    public:
      // Do not touch this implementation detail!
      typedef T _wrapper_wrapped_type_;

    protected:
      override get_override(char const* name) const
      {
        typedef detail::wrapper_base base;
        converter::registration const& r
	  = converter::registered<T>::converters;
        PyTypeObject* type = r.get_class_object();
        
        return this->base::get_override(name, type);
      }
    };
  }
}

#endif // HACKED_WRAPPER_HPP

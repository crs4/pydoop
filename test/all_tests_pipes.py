import unittest

suites = []

#--
import test_basics
suites.append(test_basics.suite())

#--
import test_task_context
suites.append(test_task_context.suite())
#--
import test_jobconf
suites.append(test_jobconf.suite())
#--
import test_factory
suites.append(test_factory.suite())

alltests = unittest.TestSuite(tuple(suites))

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(alltests)

import unittest

suites = []
#--
import test_task_context
suites.append(test_task_context.suite())
#--
import test_jobconf
suites.append(test_jobconf.suite())
#--

alltests = unittest.TestSuite(tuple(suites))

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(alltests)

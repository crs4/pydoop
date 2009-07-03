import unittest

suites = []

#--
import test_hdfs_plain_disk
suites.append(test_hdfs_plain_disk.suite())

#--
import test_hdfs_network
suites.append(test_hdfs_network.suite())

alltests = unittest.TestSuite(tuple(suites))

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(alltests)

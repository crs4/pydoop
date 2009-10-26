import unittest
import pydoop_pipes


class jobconf_tc(unittest.TestCase):
  
  def setUp(self):
    pass

  def test_override_from_python(self):
    d = {'str' : 'this is a string', 'float' : 0.23,
         'int' : 22, 'bool' : False}
    class jc(pydoop_pipes.JobConf):
      def __init__(self, d):
        pydoop_pipes.JobConf.__init__(self)
        self.d = d
      def hasKey(self, k):
        return self.d.has_key(k)
      def get(self, k):
        return self.d[k]
      def getInt(self, k):
        return int(self.get(k))
      def getFloat(self, k):
        return float(self.get(k))
      def getBoolean(self, k):
        return bool(self.get(k))
    jo = jc(d)
    jp = pydoop_pipes.wrap_JobConf_object(jo)
    for k in d:
      self.assertTrue(jp.hasKey(k))
    for k in d:
      kk = k + k + k
      self.assertFalse(jp.hasKey(kk))
    self.assertEqual(jp.get('str'), d['str'])
    self.assertEqual(jp.getFloat('float'), d['float'])
    self.assertEqual(jp.getInt('int'), d['int'])
    self.assertEqual(jp.getBoolean('bool'), d['bool'])

  def test_override_from_cpluplus(self):
    d = {'str' : 'this is a string', 'float' : '0.23',
         'int' : '22', 'bool' : 'false'}
    o = pydoop_pipes.get_JobConf_object(d)
    for k in d:
      self.assertTrue(o.hasKey(k))
    self.assertEqual(o.get('str'), d['str'])
    self.assertAlmostEqual(o.getFloat('float'), float(d['float']))
    self.assertEqual(o.getInt('int'), int(d['int']))
    self.assertEqual(o.getBoolean('bool'), d['bool'] == 'true')


def suite():
  suite = unittest.TestSuite()
  suite.addTest(jobconf_tc('test_override_from_python'))
  suite.addTest(jobconf_tc('test_override_from_cpluplus'))
  return suite


if __name__ == '__main__':
  runner = unittest.TextTestRunner(verbosity=2)
  runner.run((suite()))

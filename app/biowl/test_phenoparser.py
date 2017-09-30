import unittest
from phenoparser import PhenoWLParser, PythonGrammar

class TestGrammar(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def setUp(self):
        self.parser = PhenoWLParser(PythonGrammar())
    
    def tearDown(self):
        pass
                
    def test_identifier(self):
        test_program = '''
            _az12
        '''
        tokens = self.parser.grammar.identifier.parseString(test_program)
        self.assertTrue(tokens, "token empty")
    
    @unittest.expectedFailure    
    def test_identifier_fail(self):
        '''
        This test fails due to wrong identifier format (shouldn't start with number)
        '''
        test_program = '''
            1_az12
        '''
        tokens = self.parser.grammar.identifier.parseString(test_program)

    def test_numexpr(self):
        test_program = '''
            x20 + 30 * 5
        '''
        tokens = self.parser.grammar.numexpr.parseString(test_program)
        self.assertTrue(tokens, "token empty")
        
if __name__ == '__main__':
    unittest.main()
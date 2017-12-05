from ...provmod import ProvModel
from ...provmod import configuration

class BioProv(ProvModel.Module):
    def body(self):
        eval_lambda = self.P[0]
        return (ProvModel.Object(eval_lambda()),)
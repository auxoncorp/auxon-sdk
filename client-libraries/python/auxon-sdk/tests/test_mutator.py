from auxon_sdk import MutatorParam, mutator, MutatorHost

class MyMutatorParams:
    p = MutatorParam("p", float,
                     description = "My special parameter",
                     default_value = 7.0, value_min = 2.0, value_max = 99.0)

@mutator(name="MyMutator",
         description="My special mutator",
         operation="corrupt",
         organization_name_segment="mycorp",
         organization_custom_metadata={"my_key": "my_val"})
class MyMutator:
    def __init__(self, value: str):
        self.value = value
    
    def inject(self, mutation_id: str, params: MyMutatorParams): 
        print("Injected " + mutation_id + ", p=" + params.p + ", value=" + self.value)

    def clear_mutation(self, mutation_id: str): 
        print("Injected " + mutation_id + ", p=" + params.p + ", value=" + self.value)

    def reset(self):
        print("Reset")


def test_param_getters():
    params = MyMutatorParams()

    # default value
    assert params.p == 7.0

    # the rust code does this automatically when injecting a mutation
    params._mutator_parameters = { "p": 42.0 }
    assert params.p == 42.0


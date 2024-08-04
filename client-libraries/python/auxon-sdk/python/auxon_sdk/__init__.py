from ._auxon_sdk import *


def mutator(
    name=None,
    description=None,
    layer=None,
    group=None,
    operation=None,
    statefulness=None,
    organization_name_segment=None,
    organization_custom_metadata=None,
    params=None,
):
    def wrap(cls):
        param_descriptors = []
        if params:
            for key, ty in params.__dict__.items():
                if isinstance(ty, MutatorParam):
                    param_descriptors.append(ty)

        descriptor = MutatorDescriptor(
            params,
            param_descriptors,
            name,
            description,
            layer,
            group,
            operation,
            statefulness,
            organization_name_segment,
            organization_custom_metadata,
        )
        cls._mutator_descriptor = descriptor
        return cls

    return wrap

#!/usr/bin/env python3

from modality.sdk import IngestClient, TimelineId, AttrVal, AttrList

# TODO fixme ref bug in AttrVal
# repro:
# change v to v0 and v1 to make it work
# otherwise it produces '1.23 @ cython-test'
def foo_func(ic):
    cb_attrs = AttrList(2)
    v = AttrVal()
    v.set_string('foo-func')
    cb_attrs[0].key = ic.declare_attr_key('event.name')
    cb_attrs[0].value = v
    v = AttrVal()
    # TODO int types
    v.set_string(str(1.23))
    cb_attrs[1].key = ic.declare_attr_key('event.foo_bar')
    cb_attrs[1].value = v
    ic.event(cb_attrs)

ic = IngestClient(tracing=True)
ic.connect()
ic.authenticate("00000000000000000000000000000000")

t_attrs = AttrList(2)

tid = TimelineId()
v = AttrVal()
v.set_timeline_id(tid)
t_attrs[0].key = ic.declare_attr_key('timeline.id')
t_attrs[0].value = v
v = AttrVal()
v.set_string('cython-test')
t_attrs[1].key = ic.declare_attr_key('timeline.name')
t_attrs[1].value = v

ic.open_timeline(tid, t_attrs)

e_attrs = AttrList(2)
v = AttrVal()
v.set_string('event-a')
e_attrs[0].key = ic.declare_attr_key('event.name')
e_attrs[0].value = v
v = AttrVal()
v.set_string('bar')
e_attrs[1].key = ic.declare_attr_key('event.foo')
e_attrs[1].value = v
ic.event(e_attrs)

e_attrs = AttrList(1)
v = AttrVal()
v.set_string('event-b')
e_attrs[0].key = ic.declare_attr_key('event.name')
e_attrs[0].value = v
ic.event(e_attrs)

foo_func(ic)

ic.close_timeline()

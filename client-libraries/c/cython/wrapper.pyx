import os
from libc.stdlib cimport malloc, free

cdef class TimelineId:
    cdef modality_timeline_id inner

    def __cinit__(self):
        if modality_timeline_id_init(&self.inner) != 0:
            raise ValueError

cdef class AttrVal:
    cdef modality_attr_val inner

    def __cinit__(self):
        if modality_attr_val_set_integer(&self.inner, 0) != 0:
            raise ValueError

    def __dealloc__(self):
        if modality_attr_val_free_copy(&self.inner) != 0:
            raise ValueError

    def set_timeline_id(self, val: TimelineId):
        if modality_attr_val_free_copy(&self.inner) != 0:
            raise ValueError
        if modality_attr_val_copy_timeline_id(&self.inner, &val.inner) != 0:
            raise ValueError

    def set_string(self, val: str):
        if modality_attr_val_free_copy(&self.inner) != 0:
            raise ValueError
        if modality_attr_val_copy_string(&self.inner, bytes(val, encoding='utf8')) != 0:
            raise ValueError

cdef class Attr:
    cdef modality_attr *inner

    def __cinit__(self):
        self.inner = NULL

    def __dealloc__(self):
        self.inner = NULL

    @property
    def key(self):
        return self.inner.key

    @key.setter
    def key(self, value: modality_interned_attr_key):
        self.inner.key = value

    @property
    def value(self):
        v = AttrVal()
        v.inner = self.inner.val
        return v

    @value.setter
    def value(self, value: AttrVal):
        self.inner.val = value.inner

cdef class AttrList:
    cdef int size
    cdef modality_attr *arr

    def __cinit__(self, size):
        self.size = size
        self.arr = <modality_attr*>malloc(size * sizeof(modality_attr))

    def __dealloc__(self):
        free(self.arr)

    def __getitem__(self, index):
        if index < 0 or index >= self.size:
            raise IndexError("list index out of range")
        res = Attr()
        res.inner = &self.arr[index]
        return res

cdef class IngestClient:
    cdef modality_runtime *rt
    cdef modality_ingest_client *ic
    cdef interned_attr_keys
    cdef local_ordering

    def __cinit__(self, tracing=False):
        self.interned_attr_keys = dict()
        self.local_ordering = 0

        if tracing:
            modality_tracing_subscriber_init()

        if modality_runtime_new(&self.rt) != 0:
            raise ValueError

        if modality_ingest_client_new(self.rt, &self.ic) != 0:
            raise ValueError

    def __dealloc__(self):
        modality_ingest_client_free(self.ic)
        modality_runtime_free(self.rt)

    def connect(self, url='modality-ingest://localhost:14182', allow_insecure_tls=True, timeout_seconds=1):
        if modality_ingest_client_connect_with_timeout(
                self.ic,
                bytes(url, encoding='utf8'),
                allow_insecure_tls,
                timeout_seconds) != 0:
            raise ValueError

    def authenticate(self, auth_token=None):
        if auth_token is None:
            auth_token = os.environ.get('MODALITY_AUTH_TOKEN')
        if modality_ingest_client_authenticate(
                self.ic,
                bytes(auth_token, encoding='utf8')) != 0:
            raise ValueError

    def declare_attr_key(self, key_name):
        cdef modality_interned_attr_key interned_key
        if key_name not in self.interned_attr_keys:
            if modality_ingest_client_declare_attr_key(
                    self.ic,
                    bytes(key_name, encoding='utf8'),
                    &interned_key) != 0:
                raise ValueError
            self.interned_attr_keys[key_name] = interned_key
            return interned_key
        else:
            return self.interned_attr_keys[key_name]

    def open_timeline(self, tid: TimelineId, attrs: AttrList):
        if modality_ingest_client_open_timeline(
                self.ic,
                &tid.inner) != 0:
            raise ValueError
        if modality_ingest_client_timeline_metadata(
                self.ic,
                attrs.arr,
                attrs.size) != 0:
            raise ValueError

    def close_timeline(self):
        self.local_ordering = 0
        if modality_ingest_client_close_timeline(self.ic) != 0:
            raise ValueError

    def event(self, attrs: AttrList, ordering=None):
        if ordering is None:
            ordering = self.local_ordering
            self.local_ordering += 1
        if modality_ingest_client_event(
                self.ic,
                ordering,
                0,
                attrs.arr,
                attrs.size) != 0:
            raise ValueError
